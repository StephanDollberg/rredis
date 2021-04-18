use std::rc::Rc;
use std::cell::{RefCell};
use std::fmt;

pub struct BufferPoolAllocatorImpl {
    bufpool: Vec<Rc<RefCell<Box<[u8]>>>>,
}

impl BufferPoolAllocatorImpl {
    pub fn new() -> BufferPoolAllocatorImpl {
        return BufferPoolAllocatorImpl {
            bufpool: Vec::with_capacity(64),
        }
    }

    pub fn allocate_buf(&mut self) -> Rc<RefCell<Box<[u8]>>> {
        return match self.bufpool.pop() {
            Some(buf) => buf,
            None => {
                Rc::from(RefCell::new(Box::from(vec![0u8; 2048])))
            }
        };
    }

    pub fn deallocate_buf(&mut self, buf: Rc<RefCell<Box<[u8]>>>) {
        self.bufpool.push(buf);
    }
}

pub type BufferPoolAllocator = Rc<RefCell<BufferPoolAllocatorImpl>>;

pub fn make_buffer_pool_allocator() -> BufferPoolAllocator {
    return Rc::new(RefCell::new(BufferPoolAllocatorImpl::new()));
}

#[derive(Clone)]
pub struct BufWrap {
    pub buf: Rc<RefCell<Box<[u8]>>>,
    allocator: BufferPoolAllocator,
}

impl Drop for BufWrap {
    fn drop(&mut self) {
        self.allocator.borrow_mut().deallocate_buf(self.buf.clone());
    }
}

impl fmt::Debug for BufWrap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufWrap")
            .field("addr", & (&self.buf.borrow()[0] as *const u8))
            .finish()
    }
}

pub fn allocate_buf(allocator: BufferPoolAllocator) -> BufWrap {
    let buf = allocator.borrow_mut().allocate_buf();
    return BufWrap {
        buf,
        allocator,
    }
}
