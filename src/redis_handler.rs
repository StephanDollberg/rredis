use std::collections::HashMap;

use crate::reusable_slab_allocator::ReusableSlabAllocator;

type CommandHandler = fn(&mut RedisHandler, &Vec<redis_protocol::prelude::Frame>, &mut ReusableSlabAllocator, consumed: usize) -> HandleResult;

pub enum HandleResult {
    NotEnoughData,
    // buf_index to send, buf len and bytes consumed, TODO make this a tuple
    Processed((usize, usize, usize)),
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

        let buf_index = buf_alloc.allocate_buf();

        let bytes_written = match self.state.get(key) {
            Some(val) => {
                redis_protocol::prelude::encode(&mut buf_alloc.index(buf_index),
                                                &redis_protocol::prelude::Frame::BulkString(val.clone())).unwrap()
            }
            None => {
                redis_protocol::prelude::encode(&mut buf_alloc.index(buf_index),
                                                &redis_protocol::prelude::Frame::Null).unwrap()
            }
        };

        return HandleResult::Processed((buf_index, bytes_written, consumed));
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

        let buf_index = buf_alloc.allocate_buf();

        let bytes_written = redis_protocol::prelude::encode(&mut buf_alloc.index(buf_index),
                                                            &redis_protocol::prelude::Frame::SimpleString(String::from("OK"))).unwrap();

        return HandleResult::Processed((buf_index, bytes_written, consumed));
    }

    pub fn handle_command(&mut self, _args: &Vec<redis_protocol::prelude::Frame>, buf_alloc: &mut ReusableSlabAllocator, consumed: usize) -> HandleResult {
        let buf_index = buf_alloc.allocate_buf();

        let bytes_written = redis_protocol::prelude::encode(&mut buf_alloc.index(buf_index),
                                                            &redis_protocol::prelude::Frame::Array(Vec::new())).unwrap();

        return HandleResult::Processed((buf_index, bytes_written, consumed));
    }

    pub fn new() -> RedisHandler {
        let command_handlers = HashMap::new();

        let mut handler = RedisHandler {
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
            }
            kind => {
                println!("got unknown kind: {:?} ignoring", kind);
                return HandleResult::Error;
            }
        }
    }

    pub fn handle_data(&mut self, mut buf_alloc: &mut ReusableSlabAllocator, buf_index: usize, offset: usize, len: usize) -> HandleResult {
        match redis_protocol::prelude::decode(&buf_alloc.index(buf_index)[offset..offset+len]) {
            Ok((frame, consumed)) => {
                match frame {
                    Some(frame) => {
                        return self.handle_frame(&frame, &mut buf_alloc, consumed);
                    }
                    None => {
                        println!("Incomplete command, need more data...");
                        return HandleResult::NotEnoughData;
                    }
                }
            }
            Err(e) => {
                println!("Error parsing redis command: {:?}", e);
                return HandleResult::Error;
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use crate::redis_handler::{RedisHandler, HandleResult};
    use crate::reusable_slab_allocator::ReusableSlabAllocator;

    #[test]
    fn parse_get_set() {
        let mut handler = RedisHandler::new();
        let mut allocator = ReusableSlabAllocator::new();

        {
            let set_buf_index = allocator.allocate_buf();
            let set_data = "*3\r\n$3\r\nSET\r\n$4\r\nswag\r\n$4\r\nyolo\r\n";
            allocator.index(set_buf_index)[..set_data.len()].copy_from_slice(set_data.as_bytes());

            match handler.handle_data(&mut allocator, set_buf_index, 0, set_data.len()) {
                HandleResult::Processed((write_buf_index, write_len, bytes_consumed)) => {
                    assert_eq!(bytes_consumed, set_data.len());
                    let ok_string = "+OK\r\n";
                    assert_eq!(write_len, ok_string.len());
                    assert_eq!(&allocator.index(write_buf_index)[..write_len], ok_string.as_bytes());
                }
                _ => {
                    assert!(false, "got wrong HandleResult");
                }
            }
        }

        {
            let get_buf_index = allocator.allocate_buf();
            let get_data = "*2\r\n$3\r\nGET\r\n$4\r\nswag\r\n";
            allocator.index(get_buf_index)[..get_data.len()].copy_from_slice(get_data.as_bytes());

            match handler.handle_data(&mut allocator, get_buf_index, 0, get_data.len()) {
                HandleResult::Processed((write_buf_index, write_len, bytes_consumed)) => {
                    assert_eq!(bytes_consumed, get_data.len());
                    let resp_string = "$4\r\nyolo\r\n";
                    assert_eq!(write_len, resp_string.len());
                    assert_eq!(&allocator.index(write_buf_index)[..write_len], resp_string.as_bytes());
                }
                _ => {
                    assert!(false, "got wrong HandleResult");
                }
            }
        }
    }

    // TODO: test partial command
}
