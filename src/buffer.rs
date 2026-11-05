use bytes::{Buf, BytesMut};

pub struct RingBuffer {
    storage: BytesMut,
    capacity: usize,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            storage: BytesMut::with_capacity(capacity),
            capacity,
        }
    }

    pub fn extend_from_slice(&mut self, chunk: &[u8]) -> Result<(), &'static str> {
        if self.storage.len() + chunk.len() > self.capacity {
            return Err("Buffer overflow: chunk exceeds capacity");
        }
        self.storage.extend_from_slice(chunk);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.storage.len()
    }

    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.storage[..]
    }

    pub fn advance(&mut self, cnt: usize) {
        self.storage.advance(cnt);
    }

    pub fn clear(&mut self) {
        self.storage.clear();
    }
}
