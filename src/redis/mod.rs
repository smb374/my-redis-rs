mod handler;

use std::sync::Arc;

use evmap::{ReadHandleFactory, WriteHandle};
use handler::Entry;
use tokio::sync::Mutex;

pub struct Redis {
    reader: ReadHandleFactory<Arc<str>, Entry>,
    writer: Mutex<WriteHandle<Arc<str>, Entry>>,
}

impl Redis {
    pub fn new() -> Self {
        let (reader, writer) = evmap::new();
        Self {
            reader: reader.factory(),
            writer: Mutex::new(writer),
        }
    }
}
