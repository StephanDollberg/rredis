use std::collections::HashMap;

use crate::reusable_slab_allocator::{ReusableSlabAllocator, BufferWrapper};

type CommandHandler = fn(&mut RedisHandler, &Vec<redis_protocol::prelude::Frame>, &mut ReusableSlabAllocator, consumed: usize) -> HandleResult;

pub enum HandleResult {
    NotEnoughData,
    // buf_wrap to send, buf len and bytes consumed, TODO make this a tuple
    Processed((BufferWrapper, usize, usize)),
    Error,
}

pub struct RedisHandler {
    command_handlers: HashMap<Vec<u8>, CommandHandler>,
    state: HashMap<Vec<u8>, Vec<u8>>,
}

impl RedisHandler {
    pub fn handle_get(&mut self, args: &Vec<redis_protocol::prelude::Frame>, buf_alloc: &mut ReusableSlabAllocator, consumed: usize) -> HandleResult {
        if args.len() < 2 {
            return HandleResult::Error;
        }

        let key = match &args[1] {
            redis_protocol::prelude::Frame::BulkString(key) => key,
            _ => return HandleResult::Error
        };

        let mut buf_wrap = buf_alloc.allocate_buf();

        let bytes_written = match self.state.get(key) {
            Some(val) => {
                let mut res = 0;
                buf_wrap.index(|buf: &mut Box<[u8]>| {
                    res = redis_protocol::prelude::encode(&mut buf[..],
                                                    &redis_protocol::prelude::Frame::BulkString(val.clone())).unwrap()
                });
                res
            },
            None => {
                let mut res = 0;
                buf_wrap.index(|buf: &mut Box<[u8]>| {
                    res = redis_protocol::prelude::encode(&mut buf[..],
                                                    &redis_protocol::prelude::Frame::Null).unwrap()
                });
                res
            }
        };

        return HandleResult::Processed((buf_wrap, bytes_written, consumed));
    }

    pub fn handle_set(&mut self, args: &Vec<redis_protocol::prelude::Frame>, buf_alloc: &mut ReusableSlabAllocator, consumed: usize) -> HandleResult {
        if args.len() < 3 {
            return HandleResult::Error;
        }

        let key = match &args[1] {
            redis_protocol::prelude::Frame::BulkString(key) => key,
            _ => return HandleResult::Error
        };

        let value = match &args[2] {
            redis_protocol::prelude::Frame::BulkString(value) => value,
            _ => return HandleResult::Error
        };

        self.state.insert(key.to_vec(), value.to_vec());

        let mut buf_wrap = buf_alloc.allocate_buf();

        let bytes_written = 0;


        buf_wrap.index(|buf: &mut Box<[u8]>| {
            redis_protocol::prelude::encode(&mut buf[..],
                                            &redis_protocol::prelude::Frame::SimpleString(String::from("OK"))).unwrap();
        });

        return HandleResult::Processed((buf_wrap, bytes_written, consumed));
    }

    pub fn handle_command(&mut self, _args: &Vec<redis_protocol::prelude::Frame>, buf_alloc: &mut ReusableSlabAllocator, consumed: usize) -> HandleResult {
        let mut buf_wrap = buf_alloc.allocate_buf();

        let mut bytes_written = 0;
        buf_wrap.index(|buf: &mut Box<[u8]>| {
            bytes_written = redis_protocol::prelude::encode(&mut buf[..],
                                                                &redis_protocol::prelude::Frame::Array(Vec::new())).unwrap();
        });

        return HandleResult::Processed((buf_wrap, bytes_written, consumed));
    }

    pub fn new() -> RedisHandler {
        let command_handlers = HashMap::new();

        let mut handler = RedisHandler{
            command_handlers,
            state: HashMap::new(),
        };

        handler.command_handlers.insert("GET".as_bytes().to_vec(), RedisHandler::handle_get);
        handler.command_handlers.insert("SET".as_bytes().to_vec(), RedisHandler::handle_set);
        handler.command_handlers.insert("COMMAND".as_bytes().to_vec(), RedisHandler::handle_command);

        return handler;
    }

    fn handle_frame(&mut self, frame: &redis_protocol::prelude::Frame, mut buf_alloc: &mut ReusableSlabAllocator, consumed: usize) -> HandleResult {
        match frame {
            redis_protocol::prelude::Frame::Array(vec) => {
                let maybe_command = &vec[0];
                match maybe_command {
                    redis_protocol::prelude::Frame::BulkString(command) => {
                        println!("command {:?}", std::str::from_utf8(command));

                        let handler = self.command_handlers.get(command);

                        if handler.is_some() {
                            return self.command_handlers[command](self, &vec, &mut buf_alloc, consumed);
                        }

                        return HandleResult::Error;
                    }
                    _ => {
                        println!("got wrong command type");
                        return HandleResult::Error;
                    }
                };

            },
            kind => {
                println!("got unknown kind: {:?} ignoring", kind);
                return HandleResult::Error;
            }
        }
    }

    pub fn handle_data(&mut self, mut buf_alloc: &mut ReusableSlabAllocator, buf_wrap: &BufferWrapper, len: usize) -> HandleResult {
        let mut result: HandleResult = HandleResult::NotEnoughData;
        buf_wrap.index_non_mut(|buf: &Box<[u8]>| {
            match redis_protocol::prelude::decode(&buf[0..len]) {
                Ok((frame, consumed)) => {
                    match frame {
                        Some(frame) => {
                            result = self.handle_frame(&frame, &mut buf_alloc, consumed);
                        }
                        None => {
                            println!("Incomplete command, need more data...");
                            result = HandleResult::NotEnoughData;
                        }
                    }
                },
                Err(e) => {
                    println!("Error parsing redis command: {:?}", e);
                    result = HandleResult::Error;
                }
            };
        });

        return result;
    }
}