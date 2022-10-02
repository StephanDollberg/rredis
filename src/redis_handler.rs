use std::borrow::{Borrow};
use std::collections::HashMap;
use redis_protocol::resp2::prelude::*;
use bytes::Bytes;

use crate::reusable_slab_allocator::*;

type CommandHandler = fn(&mut RedisHandler, &Vec<Frame>, &mut BufferPoolAllocator, consumed: usize) -> HandleResult;

pub struct ProcessedResult {
    pub response_buf: BufWrapView,
    pub bytes_consumed: usize,
    pub write_wal: bool,
}

pub enum HandleResult {
    NotEnoughData,
    // buf_index to send, buf len, bytes consumed, write_wal, TODO make this a struct
    Processed(ProcessedResult),
    Error,
}

pub struct RedisHandler {
    command_handlers: HashMap<Vec<u8>, CommandHandler>,
    state: HashMap<Vec<u8>, Vec<u8>>,
}

impl RedisHandler {
    pub fn handle_get(&mut self, args: &Vec<Frame>, buf_alloc: &mut BufferPoolAllocator, consumed: usize) -> HandleResult {
        if args.len() < 2 {
            return HandleResult::Error;
        }

        let key = match &args[1] {
            Frame::BulkString(key) => key,
            _ => return HandleResult::Error
        };

        let buffer = allocate_buf(buf_alloc.clone());

        let bytes_written = match self.state.get(<bytes::Bytes as Borrow<[u8]>>::borrow(key)) {
            Some(val) => {
                encode(&mut buffer.as_ref().borrow_mut().buf, 0,
                                                &Frame::BulkString(Bytes::from(val.clone()))).unwrap()
            }
            None => {
                encode(&mut buffer.as_ref().borrow_mut().buf, 0,
                                                &Frame::Null).unwrap()
            }
        };

        return HandleResult::Processed(ProcessedResult{
            response_buf: BufWrapView::filled_from_buf_wrap(buffer, bytes_written),
            bytes_consumed: consumed,
            write_wal: false
        });
    }

    pub fn handle_set(&mut self, args: &Vec<Frame>, buf_alloc: &mut BufferPoolAllocator, consumed: usize) -> HandleResult {
        if args.len() < 3 {
            return HandleResult::Error;
        }

        let key = match &args[1] {
            Frame::BulkString(key) => key,
            _ => return HandleResult::Error
        };

        let value = match &args[2] {
            Frame::BulkString(value) => value,
            _ => return HandleResult::Error
        };

        self.state.insert(key.to_vec(), value.to_vec());

        let buffer = allocate_buf(buf_alloc.clone());

        let bytes_written = encode(&mut buffer.as_ref().borrow_mut().buf, 0,
                                                            &Frame::SimpleString(Bytes::from("OK"))).unwrap();

        return HandleResult::Processed(
            ProcessedResult {
                response_buf: BufWrapView::filled_from_buf_wrap(buffer, bytes_written),
                bytes_consumed: consumed,
                write_wal: true
            });
    }

    pub fn handle_command(&mut self, _args: &Vec<Frame>, buf_alloc: &mut BufferPoolAllocator, consumed: usize) -> HandleResult {
        let buffer = allocate_buf(buf_alloc.clone());

        let bytes_written = encode(&mut buffer.as_ref().borrow_mut().buf, 0,
                                                            &Frame::Array(Vec::new())).unwrap();

        return HandleResult::Processed(
            ProcessedResult{
                response_buf: BufWrapView::filled_from_buf_wrap(buffer, bytes_written),
                bytes_consumed: consumed,
                write_wal: false
            });
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

    fn handle_frame(&mut self, frame: &Frame, mut buf_alloc: &mut BufferPoolAllocator, consumed: usize) -> HandleResult {
        match frame {
            Frame::Array(vec) => {
                let maybe_command = &vec[0];
                match maybe_command {
                    Frame::BulkString(command) => {
                        println!("command {:?}", std::str::from_utf8(command));

                        let handler = self.command_handlers.get(<bytes::Bytes as Borrow<[u8]>>::borrow(command));

                        if handler.is_some() {
                            return self.command_handlers[<bytes::Bytes as Borrow<[u8]>>::borrow(command)](self, &vec, &mut buf_alloc, consumed);
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
        let buf = bytes::Bytes::copy_from_slice(buffer);
        return match decode(&buf) {
            Ok(maybe_frame) => {
                match maybe_frame {
                    Some((frame, consumed)) => {
                        self.handle_frame(&frame, &mut buf_alloc, consumed)
                    }
                    None => {
                        println!("Incomplete command, need more data...");
                        HandleResult::NotEnoughData
                    }
                }
            }
            Err(e) => {
                println!("Error parsing redis command: {:?}", e);
                HandleResult::Error
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
                HandleResult::Processed(result) => {
                    assert_eq!(result.bytes_consumed, set_data.len());
                    let ok_string = "+OK\r\n";
                    assert_eq!(result.response_buf.read_view().len(), ok_string.len());
                    assert_eq!(*result.response_buf.read_view(), *ok_string.as_bytes());
                    assert_eq!(result.write_wal, true);
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
                HandleResult::Processed(result) => {
                    assert_eq!(result.bytes_consumed, get_data.len());
                    let resp_string = "$4\r\nyolo\r\n";
                    assert_eq!(result.response_buf.read_view().len(), resp_string.len());
                    assert_eq!(*result.response_buf.read_view(), *resp_string.as_bytes());
                    assert_eq!(result.write_wal, false);
                }
                _ => {
                    assert!(false, "got wrong HandleResult");
                }
            }
        }
    }

    // TODO: test partial command
}
