use std::net::TcpListener;
use std::os::unix::io::{AsRawFd, RawFd};
use std::{io, ptr};

use io_uring::{opcode, types, IoUring, SubmissionQueue};
use slab::Slab;
use io_uring::types::Timespec;
use io_uring::squeue::Flags;


#[derive(Clone, Debug)]
enum Token {
    Accept,
    Read {
        fd: RawFd,
        buf_index: usize,
        offset: usize,
    },
    Write {
        fd: RawFd,
        buf_index: usize,
        offset: usize,
        len: usize,
    },
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
    backlog: Vec<io_uring::squeue::Entry>,
    bufpool: Vec<usize>,
    buf_alloc: Slab<Box<[u8]>>,
    token_alloc: Slab<Token>,
}

impl Reactor {
    pub fn new() -> anyhow::Result<Reactor> {
        let listener = TcpListener::bind(("127.0.0.1", 3456))?;

        let backlog = Vec::new();
        let bufpool = Vec::with_capacity(64);
        let buf_alloc = Slab::with_capacity(64);
        let token_alloc = Slab::with_capacity(64);

        println!("listen {}", listener.local_addr()?);


        let timespec: Timespec = Timespec::new().sec(2).nsec(1000000);

        return Ok(Reactor {
            listener,
            timespec: timespec,
            backlog: backlog,
            bufpool: bufpool,
            buf_alloc: buf_alloc,
            token_alloc: token_alloc,
        })
    }

    fn enqueue_read(&mut self, sq: &mut SubmissionQueue, token_index: usize, fd: RawFd, buf_index: usize) {
        let buf = &mut self.buf_alloc[buf_index];
        let read_e = opcode::Recv::new(types::Fd(fd), buf.as_mut_ptr(), buf.len() as _)
            .build()
            .flags(Flags::IO_LINK)
            .user_data(token_index as _);

        unsafe {
            if sq.push(&read_e).is_err() {
                println!("failed to push read");
                self.backlog.push(read_e);
            }
        }

        // link read timeout
        let timeout_e = opcode::LinkTimeout::new(&self.timespec as *const Timespec)
            .build()
            .user_data(set_timer_on_key(token_index) as _);
        unsafe {
            if sq.push(&timeout_e).is_err() {
                self.backlog.push(timeout_e);
            }
        }
    }

    fn enqueue_write(&mut self, sq: &mut SubmissionQueue, fd: RawFd, token_index: usize,
                     buf_index: usize, offset: usize, len: usize) {
        let buf = &self.buf_alloc[buf_index][offset..];
        let write_e = opcode::Send::new(types::Fd(fd), buf.as_ptr(), len as _)
            .build()
            .flags(Flags::IO_LINK)
            .user_data(token_index as _);

        unsafe {
            if sq.push(&write_e).is_err() {
                self.backlog.push(write_e);
            }
        }

        // link write timeout
        let timeout_e = opcode::LinkTimeout::new(&self.timespec as *const Timespec)
            .build()
            .user_data(set_timer_on_key(token_index) as _);
        unsafe {
            if sq.push(&timeout_e).is_err() {
                self.backlog.push(timeout_e);
            }
        }
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        let mut ring = IoUring::new(256)?;
        let (submitter, mut sq, mut cq) = ring.split();

        let accept_e = opcode::Accept::new(types::Fd(self.listener.as_raw_fd()), ptr::null_mut(), ptr::null_mut())
            .build()
            .user_data(self.token_alloc.insert(Token::Accept) as _);

        unsafe {
            sq.push(&accept_e).expect("Couldn't push first accept to queue. Something is wrong");
        }

        sq.sync();

        loop {
            println!("looping");
            match submitter.submit_and_wait(1) {
                Ok(_) => (),
                Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => (),
                Err(err) => return Err(err.into()),
            }
            cq.sync();
            println!("submitted");

            let mut iter = self.backlog.drain(..);

            // clean backlog
            loop {
                if sq.is_full() {
                    match submitter.submit() {
                        Ok(_) => (),
                        Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => break,
                        Err(err) => return Err(err.into()),
                    }
                }
                sq.sync();

                match iter.next() {
                    Some(sqe) => unsafe {
                        let _ = sq.push(&sqe);
                    },
                    None => break,
                }
            }

            drop(iter);

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

                        let token_index = self.token_alloc.insert(Token::Read{ fd, buf_index, offset: 0 });

                        self.enqueue_read(&mut sq, token_index, fd, buf_index);
                    }
                    Token::Read { fd, buf_index, offset } => {
                        if ret <= 0 {
                            eprintln!(
                                "token {:?} ret {:?} error: {:?}",
                                self.token_alloc.get(token_index),
                                ret,
                                io::Error::from_raw_os_error(-ret)
                            );
                            println!("shutdown");

                            self.bufpool.push(buf_index);
                            self.token_alloc.remove(token_index);

                            unsafe {
                                libc::close(fd);
                            }
                        } else {
                            let len = ret as usize;

                            *token = Token::Write {
                                fd,
                                buf_index,
                                len,
                                offset: 0,
                            };

                            self.enqueue_write(&mut sq, fd, token_index, buf_index, 0, len);
                        }
                    }
                    Token::Write {
                        fd,
                        buf_index,
                        offset,
                        len,
                    } => {
                        if ret <= 0 {
                            eprintln!(
                                "token {:?} ret {:?} error: {:?}",
                                self.token_alloc.get(token_index),
                                ret,
                                io::Error::from_raw_os_error(-ret)
                            );
                            println!("shutdown");

                            self.bufpool.push(buf_index);
                            self.token_alloc.remove(token_index);

                            unsafe {
                                libc::close(fd);
                            }
                        } else {
                            let write_len = ret as usize;

                            if offset + write_len >= len {
                                // enqueue read
                                *token = Token::Read { fd, buf_index, offset: 0 };

                                self.enqueue_read(&mut sq, token_index, fd, buf_index)
                            } else {
                                let offset = offset + write_len;
                                let len = len - offset;


                                *token = Token::Write {
                                    fd,
                                    buf_index,
                                    offset,
                                    len,
                                };

                                self.enqueue_write(&mut sq, fd, token_index, buf_index, offset, len);

                            };
                        }
                    }
                }
            }
        }
    }
}