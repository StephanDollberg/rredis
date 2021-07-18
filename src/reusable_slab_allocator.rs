use std::rc::Rc;
use std::cell::{Ref, RefCell, RefMut};
use std::fmt;
use std::ops::Deref;
use std::ops::DerefMut;

pub struct BufferPoolAllocatorImpl {
    bufpool: Vec<Vec<u8>>,
}

impl BufferPoolAllocatorImpl {
    pub fn new() -> BufferPoolAllocatorImpl {
        return BufferPoolAllocatorImpl {
            bufpool: Vec::with_capacity(64),
        }
    }

    pub fn allocate_buf(&mut self) -> Vec<u8> {
        return match self.bufpool.pop() {
            Some(buf) =>buf,
            None => {
                vec![0u8; 2048]
            }
        };
    }

    pub fn deallocate_buf(&mut self, buf: Vec<u8>) {
        self.bufpool.push(buf);
    }
}

pub type BufferPoolAllocator = Rc<RefCell<BufferPoolAllocatorImpl>>;

pub fn make_buffer_pool_allocator() -> BufferPoolAllocator {
    return Rc::new(RefCell::new(BufferPoolAllocatorImpl::new()));
}

#[derive(Clone)]
pub struct BufWrapImpl {
    pub buf: Vec<u8>,
    allocator: BufferPoolAllocator,
}

impl Drop for BufWrapImpl {
    fn drop(&mut self) {
        self.allocator.borrow_mut().deallocate_buf(std::mem::replace(&mut self.buf, Vec::<u8>::new()));
    }
}

pub type BufWrap = Rc<RefCell<BufWrapImpl>>;

impl fmt::Debug for BufWrapImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufWrap")
            .field("addr", & (&self.buf[0] as *const u8))
            .finish()
    }
}

pub fn allocate_buf(allocator: BufferPoolAllocator) -> BufWrap {
    let buf = allocator.borrow_mut().allocate_buf();
    return Rc::from(RefCell::new(BufWrapImpl {
        buf,
        allocator,
    }));
}

#[derive(Clone)]
pub struct BufWrapView {
    buf: BufWrap,
    read_offset: usize,
    write_offset: usize,
}

impl BufWrapView {
    pub fn read_view(&self) -> impl Deref<Target = [u8]> + '_ {
        return Ref::map(self.buf.deref().borrow(),
                        |bwi| &bwi.buf[self.read_offset..self.write_offset]);
    }

    pub fn write_view(&mut self) -> impl DerefMut<Target = [u8]> + '_ {
        return RefMut::map(self.buf.deref().borrow_mut(),
                        |bwi| &mut bwi.buf[self.write_offset..]);
    }

    pub fn is_open(&self) -> bool {
        return self.write_offset > self.read_offset;
    }

    pub fn advance_read(&mut self, advance_by: usize) {
        self.read_offset += advance_by;
    }

    pub fn advance_write(&mut self, advance_by: usize) {
        self.write_offset += advance_by;
    }

    pub fn from_buf_wrap(bw: BufWrap) -> BufWrapView {
        return BufWrapView {
            buf: bw,
            read_offset: 0,
            write_offset: 0,
        }
    }

    pub fn filled_from_buf_wrap(bw: BufWrap, written_offset: usize) -> BufWrapView {
        return BufWrapView {
            buf: bw,
            read_offset: 0,
            write_offset: written_offset,
        }
    }

    // TODO: Make readonly view
    pub fn sub_read_buf(&self, offset: usize) -> BufWrapView {
        return BufWrapView{
            buf: self.buf.clone(),
            read_offset: self.read_offset,
            write_offset: self.read_offset + offset,
        }
    }
}

impl fmt::Debug for BufWrapView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let addr = &self.buf.deref().borrow().buf[0] as *const u8;
        f.debug_struct("BufWrapView")
            .field("addr", & (addr as *const u8 ))
            .field("read_offset", &self.read_offset)
            .field("write_offset", &self.write_offset)
            .finish()
    }
}
