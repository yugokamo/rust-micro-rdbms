use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::io;
use std::ops::{Index, IndexMut};

use crate::disk::{DiskManager, PageId, PAGE_SIZE};


#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("no free buffer available in buffer pool")]
    NoFreeBuffer,
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash)]
pub struct BufferId(usize);

pub type Page = [u8; PAGE_SIZE];

#[derive(Debug)]
pub struct Buffer {
    pub page_id: PageId,
    pub page: RefCell<Page>,
    pub is_dirty: Cell<bool>,
}

impl Default for Buffer {
    fn default() -> Self {
        Self {
            page_id: Default::default(),
            page: RefCell::new([0u8; PAGE_SIZE]),
            is_dirty: Cell::new(false),
        }
    }
}

#[derive(Debug, Default)]
pub struct Frame {
    used_count: u64,
    buffer: Rc<Buffer>,
}

pub struct BufferPool {
    buffers: Vec<Frame>,
    next_victim_id: BufferId,
}

impl BufferPool {
    pub fn new(pool_size: usize) -> Self {
        let mut buffers = vec![];
        buffers.resize_with(pool_size, Default::default);
        let next_victim_id = BufferId::default();
        Self {
            buffers,
            next_victim_id,
        }
    }

    fn size(&self) -> usize {
        self.buffers.len()
    }

    // Clock-sweep algorithm
    fn evict(&mut self) -> Option<BufferId> {
        let pool_size = self.size();
        // consecutive_pinned is used for judging whether all frame is used.
        let mut consecutive_pinned = 0;
        let victim_id = loop {
            let next_victim_id = self.next_victim_id;
            let frame = &mut self[next_victim_id];
            if frame.used_count == 0 {
                break self.next_victim_id;
            }
            // NOTE: Rc::get_mut returns a mutable reference to the contained value
            // So this expression means "if the frame being not borrowed"
            if Rc::get_mut(&mut frame.buffer).is_some() {
                frame.used_count -= 1;
                consecutive_pinned = 0;
            } else {
                consecutive_pinned += 1;
                if consecutive_pinned >= pool_size {
                    return None;
                }
            }
            self.next_victim_id = self.increment_id(self.next_victim_id);
        };
        Some(victim_id)
    }

    fn increment_id(&self, buffer_id: BufferId) -> BufferId {
        // NOTE: ~.0 is tuple access in Rust
        // if buffer_id is the last one, restart from first buffer
        BufferId((buffer_id.0 + 1) % self.size())
    }
}

impl Index<BufferId> for BufferPool {
    type Output = Frame;
    fn index(&self, index: BufferId) -> &Self::Output {
        &self.buffers[index.0]
    }
}

impl IndexMut<BufferId> for BufferPool {
    fn index_mut(&mut self, index: BufferId) -> &mut Self::Output {
        &mut self.buffers[index.0]
    }
}

pub struct BufferPoolManager {
    disk_manager: DiskManager,
    buffer_pool: BufferPool,
    // The page table keeps track of pages that are currently in memory
    page_table: HashMap<PageId, BufferId>,
}

impl BufferPoolManager {
    pub fn new(disk_manager: DiskManager, buffer_pool: BufferPool) -> Self {
        let page_table = HashMap::new();
        Self {
            disk_manager,
            buffer_pool,
            page_table
        }
    }

    pub fn fetch_page(&mut self, page_id: PageId) -> Result<Rc<Buffer>, Error> {
        // If the page is in the buffer pool
        if let Some(&buffer_id) = self.page_table.get(&page_id) {
            let frame = &mut self.buffer_pool[buffer_id];
            frame.used_count += 1;
            // NOTE: Rc::clone is not deep copy.
            //       It just increment the reference count and pass the reference.
            return Ok(Rc::clone(&frame.buffer));
        }
        // If the page is not in the buffer pool, read the page from disk and save the data on buffer pool.
        // To save the page on buffer pool, make decision of which frame is available
        let buffer_id = self.buffer_pool.evict().ok_or(Error::NoFreeBuffer)?;
        let available_frame = &mut self.buffer_pool[buffer_id];
        let evict_page_id = available_frame.buffer.page_id;
        {
            // Before clearing buffer: if the buffer's data was changed (dirty flag is true), update page data in disk
            // NOTE: Option<T> can be explicitly handled via match or implicitly with unwrap.
            //       unwrap either return the inner element or panic
            // NOTE: Rc::get_mut returns a mutable reference to the contained value
            let available_buffer = Rc::get_mut(&mut available_frame.buffer).unwrap();
            if available_buffer.is_dirty.get() {
                // NOTE: ? operator early returns an Err(e)
                self.disk_manager.write_page_data(evict_page_id, available_buffer.page.get_mut())?;
            }
            // Reading the page data from disk
            available_buffer.page_id = page_id;
            available_buffer.is_dirty.set(false);
            self.disk_manager.read_page_data(page_id, available_buffer.page.get_mut())?;
            available_frame.used_count = 1;
        }

        // Updating the page table
        let page = Rc::clone(&available_frame.buffer);
        self.page_table.remove(&evict_page_id);
        self.page_table.insert(page_id, buffer_id);
        Ok(page)
    }

    pub fn create_page(&mut self) -> Result<Rc<Buffer>, Error> {
        let buffer_id = self.buffer_pool.evict().ok_or(Error::NoFreeBuffer)?;
        let available_frame = &mut self.buffer_pool[buffer_id];
        let evict_page_id = available_frame.buffer.page_id;
        let page_id = {
            let available_buffer = Rc::get_mut(&mut available_frame.buffer).unwrap();
            if available_buffer.is_dirty.get() {
                self.disk_manager.write_page_data(evict_page_id, available_buffer.page.get_mut())?;
            }
            let page_id = self.disk_manager.allocate_page();
            *available_buffer = Buffer::default();
            available_buffer.page_id = page_id;
            available_buffer.is_dirty.set(true);
            available_frame.used_count = 1;
            page_id
        };
        let page = Rc::clone(&available_frame.buffer);
        // Updating the page table
        self.page_table.remove(&evict_page_id);
        self.page_table.insert(page_id, buffer_id);
        Ok(page)
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        for (&page_id, &buffer_id) in self.page_table.iter() {
            let frame = &self.buffer_pool[buffer_id];
            let mut page = frame.buffer.page.borrow_mut();
            self.disk_manager.write_page_data(page_id, page.as_mut())?;
            frame.buffer.is_dirty.set(false);
        }
        self.disk_manager.sync()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test() {
        // create temp file
        let (data_file, data_file_path) = NamedTempFile::new().unwrap().into_parts();
        let mut disk_manager = DiskManager::new(data_file).unwrap();
        // NOTE: allocate heap memory.
        //       Vec::with_capasity creates a vector with the given capasity but with zero length.
        //       (capasity: 4096, length: 0)
        //       https://stackoverflow.com/questions/27175685/how-to-allocate-space-for-a-vect-in-rust
        let mut hello = Vec::with_capacity(PAGE_SIZE);
        // NOTE: b"foo" is a byte string expression
        //       (capasity: 4096, length: 5)
        hello.extend_from_slice(b"hello");
        // zero padding
        // NOTE: (capasity: 4096, length: 4096)
        hello.resize(PAGE_SIZE, 0);
        // allocate page on disk
        let hello_page_id = disk_manager.allocate_page();
        disk_manager.write_page_data(hello_page_id, &hello).unwrap();
        // allocate another heap memory
        let mut world = Vec::with_capacity(PAGE_SIZE);
        world.extend_from_slice(b"world");
        world.resize(PAGE_SIZE, 0);
        let world_page_id = disk_manager.allocate_page();
        disk_manager.write_page_data(world_page_id, &world).unwrap();
        // remove disk manager
        drop(disk_manager);
        // create new disk manager
        let mut disk_manager2 = DiskManager::open(&data_file_path).unwrap();
        // NOTE: (capasity:4096, length:4096)
        let mut buffer = vec![0; PAGE_SIZE];
        disk_manager2.read_page_data(hello_page_id, &mut buffer).unwrap();
        assert_eq!(hello, buffer);
        disk_manager2.read_page_data(world_page_id, &mut buffer).unwrap();
        assert_eq!(world, buffer);
    }
}