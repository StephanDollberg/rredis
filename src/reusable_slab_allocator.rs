use std::rc::Rc;
use std::cell::{RefCell};
use std::fmt;

pub struct BufferPoolAllocatorImpl {
    bufpool: Vec<Box<[u8]>>,
}

impl BufferPoolAllocatorImpl {
    pub fn new() -> BufferPoolAllocatorImpl {
        return BufferPoolAllocatorImpl {
            bufpool: Vec::with_capacity(64),
        }
    }

    pub fn allocate_buf(&mut self) -> Box<[u8]> {
        return match self.bufpool.pop() {
            Some(buf) =>buf,
            None => {
                Box::from(vec![0u8; 2048])
            }
        };
    }

    pub fn deallocate_buf(&mut self, buf: Box<[u8]>) {
        self.bufpool.push(buf);
    }
}

pub type BufferPoolAllocator = Rc<RefCell<BufferPoolAllocatorImpl>>;

pub fn make_buffer_pool_allocator() -> BufferPoolAllocator {
    return Rc::new(RefCell::new(BufferPoolAllocatorImpl::new()));
}

#[derive(Clone)]
pub struct BufWrapImpl {
    pub buf: Option<Box<[u8]>>,
    allocator: BufferPoolAllocator,
}

impl Drop for BufWrapImpl {
    fn drop(&mut self) {
        self.allocator.borrow_mut().deallocate_buf(std::mem::replace(&mut self.buf, Option::None).unwrap());
    }
}

pub type BufWrap = Rc<RefCell<BufWrapImpl>>;

impl fmt::Debug for BufWrapImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufWrap")
            .field("addr", & (&self.buf.as_ref().unwrap()[0] as *const u8))
            .finish()
    }
}

pub fn allocate_buf(allocator: BufferPoolAllocator) -> BufWrap {
    let buf = allocator.borrow_mut().allocate_buf();
    return Rc::from(RefCell::new(BufWrapImpl {
        buf: Option::Some(buf),
        allocator,
    }));
}
