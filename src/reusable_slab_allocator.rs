use slab::Slab;

pub struct ReusableSlabAllocator {
    bufpool: Vec<usize>,
    buf_alloc: Slab<Box<[u8]>>,
}

impl ReusableSlabAllocator {
    pub fn new() -> ReusableSlabAllocator {
        return ReusableSlabAllocator {
            bufpool: Vec::with_capacity(64),
            buf_alloc: Slab::with_capacity(64),
        }
    }

    pub fn allocate_buf(&mut self) -> usize {
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

    pub fn deallocate_buf(&mut self, buf_index: usize) {
        self.bufpool.push(buf_index);
    }

    pub fn index(&mut self, buf_index: usize) -> &mut Box<[u8]> {
        return &mut self.buf_alloc[buf_index];
    }
}
