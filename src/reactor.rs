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
use std::fs::{File, OpenOptions};
use std::io::Read;

extern crate redis_protocol;

// user data for io-uring calls, tracks which operation completed and links back to the context
#[derive(Clone, Debug)]
enum Token {
    Accept,
    Read(usize), // context index
    Write(usize), // context index
    WalWrite,
}

// Current works like this:
// For each connection we have one context which can have one outstanding read and write
// Each is inserted into iouring with the read or write token whose index points into the context slab
// Context has a separate read and write buffer

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

struct WalWriteContext {
    buf_wrap: BufWrap,
    len: usize,
    written: usize,
}

struct WalQueueEntry {
    entry: BufWrap,
    offset: usize,
    len: usize,
}

pub struct Reactor {
    listener: TcpListener,
    timespec: Timespec,
    backlog: VecDeque<io_uring::squeue::Entry>,
    buf_alloc: BufferPoolAllocator,
    token_alloc: Slab<Token>,
    context_alloc: Slab<Context>,
    redis: RedisHandler,
    wal_file: File,
    wal_write: Option<WalWriteContext>, // active WAL write
    wal_token_index: usize,
    wal_backlog: VecDeque<WalQueueEntry>,
}

// Helper functions so that we can encode some extra data in the io-uring user data together with the token index
// We set bits in the upper 16 bits of the token index
// So far mostly needed for timer io-uring calls. We use the same token index but set a timer flag bit

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

#[derive(Clone)]
pub struct ReactorOptions {
    pub wal_file: std::path::PathBuf,
}

impl ReactorOptions {
    fn new() -> ReactorOptions {
        return ReactorOptions {
            wal_file: std::path::PathBuf::from("wal_log.dat"),
        }
    }
}

impl Reactor {
    pub fn new() ->anyhow::Result<Reactor> {
        let listener = TcpListener::bind(("127.0.0.1", 3456))?;
        return Reactor::new_with_listener(listener, ReactorOptions::new());
    }

    pub fn new_with_listener(listener: TcpListener, options: ReactorOptions) -> anyhow::Result<Reactor> {
        let backlog = VecDeque::new();
        let mut token_alloc = Slab::with_capacity(64);

        println!("listen {}", listener.local_addr()?);

        let wal_token_index = token_alloc.insert(Token::WalWrite);

        let timespec: Timespec = Timespec::new().sec(300).nsec(1000000);

        let mut redis_handler = RedisHandler::new();
        let buf_alloc = make_buffer_pool_allocator();

        Reactor::restore_redis_from_wal(&mut redis_handler, buf_alloc.clone(), &options);

        return Ok(Reactor {
            listener,
            timespec,
            backlog,
            buf_alloc,
            token_alloc,
            context_alloc: Slab::with_capacity(64),
            redis: redis_handler,
            wal_file: OpenOptions::new().write(true).create(true).append(true).open(options.wal_file)
                .expect("Failed to open WAL log"),
            wal_write: Option::None,
            wal_token_index,
            wal_backlog: VecDeque::new(),
        })
    }

    fn restore_redis_from_wal(redis_handler: &mut RedisHandler, mut buf_alloc: BufferPoolAllocator,
                              options: &ReactorOptions) {
        match File::open(&options.wal_file) {
            Err(_) => {
                println!("No WAL file found, not restoring any data");
                return
            },
            Ok(mut file) => {
                println!("Loading WAL ...");
                let mut content = vec![];
                file.read_to_end(&mut content).unwrap();

                let mut offset = 0;

                while offset < content.len() {
                    match redis_handler.handle_data_from_slice(&mut buf_alloc, &content[offset..])  {
                        HandleResult::NotEnoughData => {
                            println!("WARN: WAL is truncated");
                            return;
                        },
                        HandleResult::Processed((_write_buf, _bytes_written, bytes_consumed, _write_wal)) => {
                            offset += bytes_consumed;
                        },
                        HandleResult::Error => {
                            panic!("Got redis protocol error when reading WAL");
                        }
                    }
                }
            },
        };
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

    fn handle_wal_write(&mut self, sq: &mut SubmissionQueue, buf_wrap: BufWrap, offset: usize, len: usize) {
        if self.wal_write.is_none() {
            let wal_write_entry = opcode::Write::new(types::Fd(self.wal_file.as_raw_fd()),
                                                    buf_wrap.borrow().buf.as_ref().unwrap()[offset..].as_ptr(), len as _)
                .build()
                .user_data(self.wal_token_index as _);

            unsafe {
                if sq.push(&wal_write_entry).is_err() {
                    self.backlog.push_back(wal_write_entry);
                }
            }

            self.wal_write = Option::Some(WalWriteContext{
                len,
                written: offset,
                buf_wrap,
            });
        } else {
            self.wal_backlog.push_back(WalQueueEntry{
                len,
                offset,
                entry: buf_wrap,
            });
        }
    }

    fn handle_data(&mut self, mut sq: &mut SubmissionQueue, fd: RawFd, context_index: usize) {
        let offset = self.context_alloc[context_index].read_buf_offset;
        let len= self.context_alloc[context_index].read_buf_len;
        let read_token_index = self.context_alloc[context_index].read_token_index;

        match self.redis.handle_data_from_buf_wrap(&mut self.buf_alloc,
                                                   &self.context_alloc[context_index].read_buf,
                                                   offset,
                                                   len)  {
            HandleResult::NotEnoughData => {
                self.context_alloc[context_index].read_buf_offset = offset + len;

                self.enqueue_read(&mut sq, read_token_index, fd,
                                  context_index, offset + len);
            },
            HandleResult::Processed((write_buf, bytes_written, bytes_consumed, write_wal)) => {
                if write_wal {
                    self.handle_wal_write(&mut sq, self.context_alloc[context_index].read_buf.clone(),
                                          self.context_alloc[context_index].read_buf_offset, bytes_consumed);
                }

                self.context_alloc[context_index].read_buf_len -= bytes_consumed;
                self.context_alloc[context_index].read_buf_offset += bytes_consumed;
                self.context_alloc[context_index].write_len = bytes_written;
                self.context_alloc[context_index].write_buf_offset = 0;
                self.context_alloc[context_index].write_buf = write_buf.clone();

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
                    Token::WalWrite => {
                        if ret <= 0 {
                            panic!("Wal write failed, aborting ...");
                        }

                        let wal_written = ret as usize;
                        let offset = wal_written + self.wal_write.as_ref().unwrap().written;
                        let len = self.wal_write.as_ref().unwrap().len;
                        let buf_wrap = self.wal_write.as_ref().unwrap().buf_wrap.clone();

                        if offset < len {
                            let to_write = len - offset;
                            let wal_write_entry = opcode::Write::new(types::Fd(self.wal_file.as_raw_fd()),
                                 buf_wrap.borrow().buf.as_ref().unwrap()[offset..].as_ptr(), to_write as _)
                                .build()
                                .user_data(self.wal_token_index as _);

                            unsafe {
                                if sq.push(&wal_write_entry).is_err() {
                                    self.backlog.push_back(wal_write_entry);
                                }
                            }

                            self.wal_write = Option::Some(WalWriteContext{
                                len,
                                written: offset,
                                buf_wrap,
                            });
                        } else {
                            self.wal_write = Option::None;

                            if !self.wal_backlog.is_empty() {
                                let queue_entry = self.wal_backlog.pop_front().unwrap();
                                self.handle_wal_write(&mut sq, queue_entry.entry, queue_entry.offset, queue_entry.len);
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
    use crate::reactor::{Reactor, ReactorOptions};
    use std::net::{TcpStream, TcpListener};
    use std::io::{Write, Read};

    #[test]
    fn basic_smoke_test() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let tmp_dir = tempfile::tempdir().unwrap();
        let options = ReactorOptions {
            wal_file: tmp_dir.path().join("wal_log"),
        };

        let wal_path = options.wal_file.clone();

        std::thread::spawn(move || {
            let mut reactor = Reactor::new_with_listener(listener, options).unwrap();
            reactor.run().unwrap();
        });

        let mut sock = TcpStream::connect(("localhost", port)).unwrap();

        let set_string = "*3\r\n$3\r\nSET\r\n$4\r\nswag\r\n$4\r\nyolo\r\n";

        {
            sock.write_all(set_string.as_bytes()).unwrap();
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

        assert_eq!(std::fs::read_to_string(wal_path.as_path()).unwrap(), set_string);
    }

    #[test]
    fn test_pipeline() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let tmp_dir = tempfile::tempdir().unwrap();
        let options = ReactorOptions {
            wal_file: tmp_dir.path().join("wal_log"),
        };

        let wal_path = options.wal_file.clone();

        std::thread::spawn(move || {
            let mut reactor = Reactor::new_with_listener(listener, options).unwrap();
            reactor.run().unwrap();
        });

        let mut sock = TcpStream::connect(("localhost", port)).unwrap();

        let set_string = "*3\r\n$3\r\nSET\r\n$4\r\nswag\r\n$4\r\nyolo\r\n";

        sock.write_all((set_string.to_owned() + "*2\r\n$3\r\nGET\r\n$4\r\nswag\r\n").as_bytes()).unwrap();

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

        assert_eq!(std::fs::read_to_string(wal_path.as_path()).unwrap(), set_string);
    }

    // TODO: test partial command
}
