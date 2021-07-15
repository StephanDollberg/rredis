use std::collections::HashMap;

use crate::reusable_slab_allocator::*;

type CommandHandler = fn(&mut RedisHandler, &Vec<redis_protocol::prelude::Frame>, &mut BufferPoolAllocator, consumed: usize) -> HandleResult;

pub enum HandleResult {
    NotEnoughData,
    // buf_index to send, buf len, bytes consumed, write_wal, TODO make this a struct
    Processed((BufWrapView, usize, bool)),
    Error,
}

pub struct RedisHandler {
    command_handlers: HashMap<Vec<u8>, CommandHandler>,
    state: HashMap<Vec<u8>, Vec<u8>>,
}

impl RedisHandler {
    pub fn handle_get(&mut self, args: &Vec<redis_protocol::prelude::Frame>, buf_alloc: &mut BufferPoolAllocator, consumed: usize) -> HandleResult {
        if args.len() < 2 {
            return HandleResult::Error;
        }

        let key = match &args[1] {
            redis_protocol::prelude::Frame::BulkString(key) => key,
            _ => return HandleResult::Error
        };

        let buffer = allocate_buf(buf_alloc.clone());

        let bytes_written = match self.state.get(key) {
            Some(val) => {
                redis_protocol::prelude::encode(&mut buffer.borrow_mut().buf.as_mut().unwrap()[..],
                                                &redis_protocol::prelude::Frame::BulkString(val.clone())).unwrap()
            }
            None => {
                redis_protocol::prelude::encode(&mut buffer.borrow_mut().buf.as_mut().unwrap()[..],
                                                &redis_protocol::prelude::Frame::Null).unwrap()
            }
        };

        return HandleResult::Processed((BufWrapView::filled_from_buf_wrap(buffer, bytes_written), consumed, false));
    }

    pub fn handle_set(&mut self, args: &Vec<redis_protocol::prelude::Frame>, buf_alloc: &mut BufferPoolAllocator, consumed: usize) -> HandleResult {
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

        let buffer = allocate_buf(buf_alloc.clone());

        let bytes_written = redis_protocol::prelude::encode(&mut buffer.borrow_mut().buf.as_mut().unwrap()[..],
                                                            &redis_protocol::prelude::Frame::SimpleString(String::from("OK"))).unwrap();

        return HandleResult::Processed((BufWrapView::filled_from_buf_wrap(buffer, bytes_written), consumed, true));
    }

    pub fn handle_command(&mut self, _args: &Vec<redis_protocol::prelude::Frame>, buf_alloc: &mut BufferPoolAllocator, consumed: usize) -> HandleResult {
        let buffer = allocate_buf(buf_alloc.clone());

        let bytes_written = redis_protocol::prelude::encode(&mut buffer.borrow_mut().buf.as_mut().unwrap()[..],
                                                            &redis_protocol::prelude::Frame::Array(Vec::new())).unwrap();

        return HandleResult::Processed((BufWrapView::filled_from_buf_wrap(buffer, bytes_written), consumed, false));
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

    fn handle_frame(&mut self, frame: &redis_protocol::prelude::Frame, mut buf_alloc: &mut BufferPoolAllocator, consumed: usize) -> HandleResult {
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

    pub fn handle_data_from_buf_wrap(&mut self, mut buf_alloc: &mut BufferPoolAllocator, buffer: &BufWrapView) -> HandleResult {
        return self.handle_data_from_slice(&mut buf_alloc, &buffer.read_view());
    }

    pub fn handle_data_from_slice(&mut self, mut buf_alloc: &mut BufferPoolAllocator, buffer: &[u8]) -> HandleResult {
        match redis_protocol::prelude::decode(buffer) {
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
    use crate::reusable_slab_allocator::*;

    #[test]
    fn parse_get_set() {
        let mut handler = RedisHandler::new();
        let mut allocator = make_buffer_pool_allocator();

        {
            let mut buffer = BufWrapView::from_buf_wrap(allocate_buf(allocator.clone()));
            let set_data = "*3\r\n$3\r\nSET\r\n$4\r\nswag\r\n$4\r\nyolo\r\n";
            buffer.write_view()[..set_data.len()].copy_from_slice(set_data.as_bytes());
            buffer.advance_write(set_data.len());

            match handler.handle_data_from_buf_wrap(&mut allocator, &buffer) {
                HandleResult::Processed((write_buffer, bytes_consumed, _wal)) => {
                    assert_eq!(bytes_consumed, set_data.len());
                    let ok_string = "+OK\r\n";
                    assert_eq!(write_buffer.read_view().len(), ok_string.len());
                    assert_eq!(*write_buffer.read_view(), *ok_string.as_bytes());
                }
                _ => {
                    assert!(false, "got wrong HandleResult");
                }
            }
        }

        {
            let mut buffer = BufWrapView::from_buf_wrap(allocate_buf(allocator.clone()));
            let get_data = "*2\r\n$3\r\nGET\r\n$4\r\nswag\r\n";
            buffer.write_view()[..get_data.len()].copy_from_slice(get_data.as_bytes());
            buffer.advance_write(get_data.len());

            match handler.handle_data_from_buf_wrap(&mut allocator, &buffer) {
                HandleResult::Processed((write_buffer, bytes_consumed, _wal)) => {
                    assert_eq!(bytes_consumed, get_data.len());
                    let resp_string = "$4\r\nyolo\r\n";
                    assert_eq!(write_buffer.read_view().len(), resp_string.len());
                    assert_eq!(*write_buffer.read_view(), *resp_string.as_bytes());
                }
                _ => {
                    assert!(false, "got wrong HandleResult");
                }
            }
        }
    }

    // TODO: test partial command
}
