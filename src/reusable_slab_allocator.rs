use slab::Slab;

use std::rc::Rc;
use std::cell::RefCell;

pub struct ReusableSlabAllocator {
    implr: Rc<RefCell<ReusableSlabAllocatorImpl>>,
}

impl ReusableSlabAllocator {
    pub fn new() -> ReusableSlabAllocator {
        return ReusableSlabAllocator {
            implr: Rc::new(RefCell::new(ReusableSlabAllocatorImpl::new()))
        }
    }

    pub fn allocate_buf(&mut self) -> BufferWrapper {
        return BufferWrapper::allocated_from(&self.implr);
    }
}

#[derive(Debug)]
pub struct ReusableSlabAllocatorImpl {
    bufpool: Vec<usize>,
    buf_alloc: Slab<Box<[u8]>>,
}

impl ReusableSlabAllocatorImpl {
    pub fn new() -> ReusableSlabAllocatorImpl {
        return ReusableSlabAllocatorImpl {
            bufpool: Vec::with_capacity(64),
            buf_alloc: Slab::with_capacity(64),
        }
    }

    fn allocate_buf(&mut self) -> usize {
        let buf_index = match self.bufpool.pop() {
            Some(buf_index) => buf_index,
            None => {
                let buf = vec![0u8; 2048].into_boxed_slice();
                let buf_entry = self.buf_alloc.vacant_entry();
                let buf_index = buf_entry.key();
                buf_entry.insert(buf);
                buf_index
            }
        };

        return buf_index;
    }

    fn deallocate_buf(&mut self, buf_index: usize) {
        self.bufpool.push(buf_index);
    }

    pub fn index(&mut self, buf_index: usize) -> &mut Box<[u8]> {
        return &mut self.buf_alloc[buf_index];
    }

    pub fn non_mut_index(&self, buf_index: usize) -> &Box<[u8]> {
        return &self.buf_alloc[buf_index];
    }
}

#[derive(Clone, Debug)]
pub struct BufferWrapper {
    buf_index: usize,
    allocator: Rc<RefCell<ReusableSlabAllocatorImpl>>,
}

impl BufferWrapper {
    fn allocated_from(allocator: &Rc<RefCell<ReusableSlabAllocatorImpl>>) -> BufferWrapper {
        let buf_index = allocator.borrow_mut().allocate_buf();
        return BufferWrapper {
            buf_index,
            allocator: allocator.clone(),
        }
    }

    pub fn index<F>(&mut self, mut func: F) where F: FnMut(&mut Box<[u8]>) {
        func(&mut self.allocator.borrow_mut().index(self.buf_index));
    }

    pub fn index_non_mut<F>(&self, mut func: F) where F: FnMut(&Box<[u8]>) {
        func(&mut self.allocator.borrow().non_mut_index(self.buf_index));
    }
}

impl Drop for BufferWrapper {
    fn drop(&mut self) {
        self.allocator.borrow_mut().deallocate_buf(self.buf_index);
    }
}