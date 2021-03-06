use std::net::TcpListener;
use std::os::unix::io::{AsRawFd, RawFd};
use std::{io, ptr};

use io_uring::{opcode, types, IoUring};
use slab::Slab;
use io_uring::types::Timespec;
use io_uring::squeue::Flags;

#[derive(Clone, Debug)]
enum Token {
    Accept,
    Read {
        fd: RawFd,
        buf_index: usize,
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

fn main() -> anyhow::Result<()> {
    let mut ring = IoUring::new(256)?;
    let listener = TcpListener::bind(("127.0.0.1", 3456))?;

    let mut backlog = Vec::new();
    let mut bufpool = Vec::with_capacity(64);
    let mut buf_alloc = Slab::with_capacity(64);
    let mut token_alloc = Slab::with_capacity(64);

    println!("listen {}", listener.local_addr()?);

    let (submitter, mut sq, mut cq) = ring.split();

    let accept_e = opcode::Accept::new(types::Fd(listener.as_raw_fd()), ptr::null_mut(), ptr::null_mut())
        .build()
        .user_data(token_alloc.insert(Token::Accept) as _);

    unsafe {
        sq.push(&accept_e).expect("Couldn't push first accept to queue. Something is wrong");
    }

    sq.sync();

    let timespec: Timespec = Timespec::new().sec(2).nsec(1000000);

    loop {
        println!("looping");
        match submitter.submit_and_wait(1) {
            Ok(_) => (),
            Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => (),
            Err(err) => return Err(err.into()),
        }
        cq.sync();
        println!("submitted");

        let mut iter = backlog.drain(..);

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

            let token = &mut token_alloc[token_index];
            println!("token {:?}", token);
            match token.clone() {
                Token::Accept => {
                    println!("accept");

                    // enqueue new accept
                    let accept_e = opcode::Accept::new(types::Fd(listener.as_raw_fd()), ptr::null_mut(), ptr::null_mut())
                        .build()
                        .user_data(token_index as _);

                    unsafe {
                        sq.push(&accept_e).expect("Couldn't push first accept to queue. Something is wrong");
                    }

                    // enqueue read
                    let fd = ret;

                    let (buf_index, buf) = match bufpool.pop() {
                        Some(buf_index) => (buf_index, &mut buf_alloc[buf_index]),
                        None => {
                            let buf = vec![0u8; 2048].into_boxed_slice();
                            let buf_entry = buf_alloc.vacant_entry();
                            let buf_index = buf_entry.key();
                            (buf_index, buf_entry.insert(buf))
                        }
                    };

                    let token_index = token_alloc.insert(Token::Read{ fd, buf_index });

                    let read_e = opcode::Recv::new(types::Fd(fd), buf.as_mut_ptr(), buf.len() as _)
                        .build()
                        .flags(Flags::IO_LINK)
                        .user_data(token_index as _);

                    unsafe {
                        if sq.push(&read_e).is_err() {
                            println!("failed to push read");
                            backlog.push(read_e);
                        }
                    }

                    // link read timeout
                    let timeout_e = opcode::LinkTimeout::new(&timespec as *const Timespec)
                        .build()
                        .user_data(set_timer_on_key(token_index) as _);
                    unsafe {
                        if sq.push(&timeout_e).is_err() {
                            backlog.push(timeout_e);
                        }
                    }
                }
                Token::Read { fd, buf_index } => {
                    if ret <= 0 {
                        eprintln!(
                            "token {:?} ret {:?} error: {:?}",
                            token_alloc.get(token_index),
                            ret,
                            io::Error::from_raw_os_error(-ret)
                        );
                        println!("shutdown");

                        bufpool.push(buf_index);
                        token_alloc.remove(token_index);

                        unsafe {
                            libc::close(fd);
                        }
                    } else {
                        let len = ret as usize;
                        let buf = &buf_alloc[buf_index];

                        *token = Token::Write {
                            fd,
                            buf_index,
                            len,
                            offset: 0,
                        };

                        let write_e = opcode::Send::new(types::Fd(fd), buf.as_ptr(), len as _)
                            .build()
                            .flags(Flags::IO_LINK)
                            .user_data(token_index as _);

                        unsafe {
                            if sq.push(&write_e).is_err() {
                                backlog.push(write_e);
                            }
                        }

                        // link write timeout
                        let timeout_e = opcode::LinkTimeout::new(&timespec as *const Timespec)
                            .build()
                            .user_data(set_timer_on_key(token_index) as _);
                        unsafe {
                            if sq.push(&timeout_e).is_err() {
                                backlog.push(timeout_e);
                            }
                        }
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
                            token_alloc.get(token_index),
                            ret,
                            io::Error::from_raw_os_error(-ret)
                        );
                        println!("shutdown");

                        bufpool.push(buf_index);
                        token_alloc.remove(token_index);

                        unsafe {
                            libc::close(fd);
                        }
                    } else {
                        let write_len = ret as usize;

                        if offset + write_len >= len {
                            // enqueue read
                            let buf= &mut buf_alloc[buf_index];

                            *token = Token::Read { fd, buf_index };

                            let read_e = opcode::Recv::new(types::Fd(fd), buf.as_mut_ptr(), buf.len() as _)
                                .build()
                                .flags(Flags::IO_LINK)
                                .user_data(token_index as _);

                            unsafe {
                                if sq.push(&read_e).is_err() {
                                    println!("failed to push read");
                                    backlog.push(read_e);
                                }
                            }

                            // link read timeout
                            let timeout_e = opcode::LinkTimeout::new(&timespec as *const Timespec)
                                .build()
                                .user_data(set_timer_on_key(token_index) as _);
                            unsafe {
                                if sq.push(&timeout_e).is_err() {
                                    backlog.push(timeout_e);
                                }
                            }
                        } else {
                            let offset = offset + write_len;
                            let len = len - offset;

                            let buf = &buf_alloc[buf_index][offset..];

                            *token = Token::Write {
                                fd,
                                buf_index,
                                offset,
                                len,
                            };

                            let entry= opcode::Write::new(types::Fd(fd), buf.as_ptr(), len as _)
                                .build()
                                .user_data(token_index as _);

                            unsafe {
                                if sq.push(&entry).is_err() {
                                    backlog.push(entry);
                                }
                            }
                        };
                    }
                }
            }
        }
    }
}
