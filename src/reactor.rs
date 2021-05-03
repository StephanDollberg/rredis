use std::net::TcpListener;
use std::os::unix::io::{AsRawFd, RawFd};
use std::{io, ptr};

use io_uring::{opcode, types, IoUring, SubmissionQueue};
use slab::Slab;
use io_uring::types::Timespec;
use io_uring::squeue::Flags;

use crate::redis_handler::RedisHandler;
use crate::redis_handler::HandleResult;
use crate::reusable_slab_allocator::*;
use std::collections::VecDeque;

extern crate redis_protocol;

#[derive(Clone, Debug)]
enum Token {
    Accept,
    Read(usize),
    Write(usize),
}

// Current works like this:
// For each connection we have one context which can have one outstanding read and write
// Each is inserted into iouring with the read or write token
// Context has a separate read and write buffer for both which are currently tracked by index (use Rc instead?)

#[derive(Clone, Debug)]
struct Context {
    // index into the token slab (for the read token)
    read_token_index: usize,

    // index into the token slab (for the write token)
    write_token_index: usize,

    fd: RawFd,

    // buffer for read ops
    read_buf: BufWrap,

    // which offset are we at in the buffer
    read_buf_offset: usize,

    // how much data we have left starting from offset
    read_buf_len: usize,

    // buffer for write ops
    write_buf: BufWrap,

    // which offset are we at in the buffer
    write_buf_offset: usize,

    // how much data we have left starting from offset
    write_len: usize,
}

fn key_tags(key: usize) -> usize {
    return key >> 48;
}

fn key_without_tags(key: usize) -> usize {
    return key & 0xffffffffffff
}

fn set_timer_on_key(key: usize) -> usize {
    return key | (1 << 48);
}

fn tag_is_timer(tag: usize) -> bool {
    return (tag & 0x1) == 0x1;
}

pub struct Reactor {
    listener: TcpListener,
    timespec: Timespec,
    backlog: VecDeque<io_uring::squeue::Entry>,
    buf_alloc: BufferPoolAllocator,
    token_alloc: Slab<Token>,
    context_alloc: Slab<Context>,
    redis: RedisHandler,
}

impl Reactor {
    pub fn new() ->anyhow::Result<Reactor> {
        let listener = TcpListener::bind(("127.0.0.1", 3456))?;
        return Reactor::new_with_listener(listener);
    }

    pub fn new_with_listener(listener: TcpListener) -> anyhow::Result<Reactor> {
        let backlog = VecDeque::new();
        let token_alloc = Slab::with_capacity(64);

        println!("listen {}", listener.local_addr()?);


        let timespec: Timespec = Timespec::new().sec(300).nsec(1000000);

        return Ok(Reactor {
            listener,
            timespec,
            backlog,
            buf_alloc: make_buffer_pool_allocator(),
            token_alloc,
            context_alloc: Slab::with_capacity(64),
            redis: RedisHandler::new(),
        })
    }

    fn enqueue_read(&mut self, sq: &mut SubmissionQueue, token_index: usize, fd: RawFd,
        context_index: usize, offset: usize) {
        let mut buf_borrow = self.context_alloc[context_index].read_buf.borrow_mut();
        let buf = &mut buf_borrow.buf.as_mut().unwrap()[offset..];
        let read_e = opcode::Recv::new(types::Fd(fd), buf.as_mut_ptr(), buf.len() as _)
            .build()
            .flags(Flags::IO_LINK)
            .user_data(token_index as _);

        unsafe {
            if sq.push(&read_e).is_err() {
                println!("failed to push read");
                self.backlog.push_back(read_e);
            }
        }

        // link read timeout
        let timeout_e = opcode::LinkTimeout::new(&self.timespec as *const Timespec)
            .build()
            .user_data(set_timer_on_key(token_index) as _);
        unsafe {
            if sq.push(&timeout_e).is_err() {
                self.backlog.push_back(timeout_e);
            }
        }
    }

    fn enqueue_write(&mut self, sq: &mut SubmissionQueue, fd: RawFd, token_index: usize,
                     context_index: usize, offset: usize, len: usize) {
        let buf_borrow = self.context_alloc[context_index].write_buf.borrow();
        let buf = &buf_borrow.buf.as_ref().unwrap()[offset..];
        let write_e = opcode::Send::new(types::Fd(fd), buf.as_ptr(), len as _)
            .build()
            .flags(Flags::IO_LINK)
            .user_data(token_index as _);

        unsafe {
            if sq.push(&write_e).is_err() {
                self.backlog.push_back(write_e);
            }
        }

        // link write timeout
        let timeout_e = opcode::LinkTimeout::new(&self.timespec as *const Timespec)
            .build()
            .user_data(set_timer_on_key(token_index) as _);
        unsafe {
            if sq.push(&timeout_e).is_err() {
                self.backlog.push_back(timeout_e);
            }
        }
    }

    fn handle_data(&mut self, mut sq: &mut SubmissionQueue, fd: RawFd, context_index: usize) {
        let offset = self.context_alloc[context_index].read_buf_offset;
        let len= self.context_alloc[context_index].read_buf_len;
        let read_token_index = self.context_alloc[context_index].read_token_index;

        match self.redis.handle_data(&mut self.buf_alloc, &self.context_alloc[context_index].read_buf, offset, len)  {
            HandleResult::NotEnoughData => {
                self.context_alloc[context_index].read_buf_offset = offset + len;

                self.enqueue_read(&mut sq, read_token_index, fd,
                                  context_index, offset + len);
            },
            HandleResult::Processed((write_buf, bytes_written, bytes_consumed)) => {
                self.context_alloc[context_index].read_buf_len -= bytes_consumed;
                self.context_alloc[context_index].read_buf_offset += bytes_consumed;
                self.context_alloc[context_index].write_len = bytes_written;
                self.context_alloc[context_index].write_buf_offset = 0;
                self.context_alloc[context_index].write_buf = write_buf;

                self.enqueue_write(&mut sq, fd, self.context_alloc[context_index].write_token_index,
                                   context_index, 0, bytes_written);
            },
            HandleResult::Error => {
                println!("Got redis protocol error, closing");

                self.token_alloc.remove(read_token_index);

                unsafe {
                    libc::close(fd);
                }
            }
        }
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        // TODO move this out so that we can just run in lockstep
        // Needs fighting with lifetimes

        let mut ring = IoUring::new(256)?;
        let (submitter, mut sq, mut cq) = ring.split();

        let accept_e = opcode::Accept::new(types::Fd(self.listener.as_raw_fd()), ptr::null_mut(), ptr::null_mut())
            .build()
            .user_data(self.token_alloc.insert(Token::Accept) as _);

        unsafe {
            sq.push(&accept_e).expect("Couldn't push first accept to queue. Something is wrong");
        }


        loop {
            println!("looping");
            sq.sync();

            match submitter.submit_and_wait(1) {
                Ok(_) => (),
                Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => (),
                Err(err) => return Err(err.into()),
            }
            cq.sync();

            println!("submitted");


            unsafe {
                while !self.backlog.is_empty() && !sq.push(self.backlog.front().unwrap()).is_err() {
                    self.backlog.pop_front();
                }
            }

            for cqe in &mut cq {
                let ret = cqe.result();
                let userdata = cqe.user_data() as usize;

                println!("event userdata {:?} ret {:?}", userdata, ret);

                let token_index = key_without_tags(userdata);
                let tags = key_tags(userdata);

                if tag_is_timer(tags)
                {
                    // nothing to do, actual event will have been cancelled if timer runs out
                    continue;
                }

                let token = &mut self.token_alloc[token_index];
                println!("token {:?}", token);
                match token.clone() {
                    Token::Accept => {
                        println!("accept");

                        // enqueue new accept
                        let accept_e = opcode::Accept::new(types::Fd(self.listener.as_raw_fd()), ptr::null_mut(), ptr::null_mut())
                            .build()
                            .user_data(token_index as _);

                        unsafe {
                            sq.push(&accept_e).expect("Couldn't push first accept to queue. Something is wrong");
                        }

                        // enqueue read

                        let fd = ret;

                        let context_index = self.context_alloc.insert(Context{
                            read_token_index: 0,
                            write_token_index: 0,
                            fd,
                            read_buf: allocate_buf(self.buf_alloc.clone()),
                            read_buf_offset: 0,
                            read_buf_len: 0,
                            write_buf: allocate_buf(self.buf_alloc.clone()),
                            write_buf_offset: 0,
                            write_len: 0,
                        });

                        self.context_alloc[context_index].read_token_index = self.token_alloc.insert(Token::Read(context_index));
                        self.context_alloc[context_index].write_token_index = self.token_alloc.insert(Token::Write(context_index));

                        self.enqueue_read(&mut sq, self.context_alloc[context_index].read_token_index, fd,
                                          context_index, 0);
                    }
                    Token::Read(context_index) => {
                        println!("read context {:?}", self.context_alloc[context_index]);
                        if ret <= 0 {
                            eprintln!(
                                "closing token {:?} ret {:?} error: {:?}",
                                self.token_alloc.get(token_index),
                                ret,
                                io::Error::from_raw_os_error(-ret)
                            );

                            unsafe {
                                libc::close(self.context_alloc[context_index].fd);
                            }

                            self.token_alloc.remove(self.context_alloc[context_index].read_token_index);
                            self.token_alloc.remove(self.context_alloc[context_index].write_token_index);
                            self.context_alloc.remove(context_index);

                        } else {
                            self.context_alloc[context_index].read_buf_len += ret as usize;
                            self.handle_data(&mut sq, self.context_alloc[context_index].fd, context_index);
                        }
                    }
                    Token::Write(context_index) => {
                        println!("write context {:?}", self.context_alloc[context_index]);
                        if ret <= 0 {
                            eprintln!(
                                "token {:?} ret {:?} error: {:?}",
                                self.token_alloc.get(token_index),
                                ret,
                                io::Error::from_raw_os_error(-ret)
                            );
                            println!("shutdown");

                            self.token_alloc.remove(self.context_alloc[context_index].read_token_index);
                            self.token_alloc.remove(self.context_alloc[context_index].write_token_index);
                            self.context_alloc.remove(context_index);

                            unsafe {
                                libc::close(self.context_alloc[context_index].fd);
                            }
                        } else {
                            let write_len = ret as usize;
                            let offset = self.context_alloc[context_index].write_buf_offset;

                            // incomplete write
                            if offset + write_len < self.context_alloc[context_index].write_len {
                                let offset = offset + write_len;
                                let len = write_len - offset;

                                *token = Token::Write(context_index);
                                self.context_alloc[context_index].write_buf_offset = offset;
                                self.context_alloc[context_index].write_len = len;

                                self.enqueue_write(&mut sq, self.context_alloc[context_index].fd, token_index,
                                                   context_index, offset, len);
                            } else {
                                // there is more unprocessed data
                                if self.context_alloc[context_index].read_buf_len != 0 {
                                    self.handle_data(&mut sq, self.context_alloc[context_index].fd, context_index);
                                }
                                // read new request
                                else {
                                    self.context_alloc[context_index].read_buf_offset = 0;

                                    self.enqueue_read(&mut sq, self.context_alloc[context_index].read_token_index,
                                        self.context_alloc[context_index].fd, context_index,
                                        self.context_alloc[context_index].read_buf_offset);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::reactor::Reactor;
    use std::net::{TcpStream, TcpListener};
    use std::io::{Write, Read};

    #[test]
    fn basic_smoke_test() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();

        std::thread::spawn(move || {
            let mut reactor = Reactor::new_with_listener(listener).unwrap();
            reactor.run().unwrap();
        });

        let mut sock = TcpStream::connect(("localhost", port)).unwrap();

        {
            sock.write_all("*3\r\n$3\r\nSET\r\n$4\r\nswag\r\n$4\r\nyolo\r\n".as_bytes()).unwrap();
            let mut buf: [u8; 1000] = [0; 1000];
            let bytes_read = sock.read(&mut buf).unwrap();

            let expected_response = "+OK\r\n";

            assert_eq!(bytes_read, expected_response.len());
            assert_eq!(expected_response.as_bytes()[..], buf[..bytes_read]);
        }

        {
            sock.write_all("*2\r\n$3\r\nGET\r\n$4\r\nswag\r\n".as_bytes()).unwrap();
            let mut buf: [u8; 1000] = [0; 1000];
            let bytes_read = sock.read(&mut buf).unwrap();

            let expected_response = "$4\r\nyolo\r\n";

            assert_eq!(bytes_read, expected_response.len());
            assert_eq!(expected_response.as_bytes()[..], buf[..bytes_read]);
        }
    }

    #[test]
    fn test_pipeline() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();

        std::thread::spawn(move || {
            let mut reactor = Reactor::new_with_listener(listener).unwrap();
            reactor.run().unwrap();
        });

        let mut sock = TcpStream::connect(("localhost", port)).unwrap();

        sock.write_all("*3\r\n$3\r\nSET\r\n$4\r\nswag\r\n$4\r\nyolo\r\n*2\r\n$3\r\nGET\r\n$4\r\nswag\r\n".as_bytes()).unwrap();

        {
            let mut buf: [u8; 1000] = [0; 1000];
            let mut bytes_read = 0;
            let expected_response = "+OK\r\n$4\r\nyolo\r\n";

            while bytes_read < expected_response.len() {
                bytes_read += sock.read(&mut buf[bytes_read..]).unwrap();
            }

            assert_eq!(bytes_read, expected_response.len());
            assert_eq!(expected_response.as_bytes()[..], buf[..bytes_read]);
        }
    }

    // TODO: test partial command
}
