use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use crate::buffer::RingBuffer;
use crate::protocol::{Frame, ProtocolDecoder};

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub buffer_capacity: usize,
    pub worker_channels: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            buffer_capacity: 1024 * 1024 * 8,
            worker_channels: 10_000,
        }
    }
}

#[derive(Default)]
pub struct EngineStats {
    pub frames_processed: AtomicU64,
    pub bytes_processed: AtomicU64,
}

pub struct StreamEngine {
    config: EngineConfig,
    stats: Arc<EngineStats>,
    sender: mpsc::Sender<Frame>,
}

impl StreamEngine {
    pub fn new(config: EngineConfig) -> (Self, mpsc::Receiver<Frame>, Arc<EngineStats>) {
        let (tx, rx) = mpsc::channel(config.worker_channels);
        let stats = Arc::new(EngineStats::default());
        let engine = Self {
            config,
            stats: Arc::clone(&stats),
            sender: tx,
        };
        (engine, rx, stats)
    }

    pub async fn ingest_chunk(&self, buffer: &mut RingBuffer, chunk: &[u8]) -> Result<usize, &'static str> {
        buffer.extend_from_slice(chunk)?;
        self.stats.bytes_processed.fetch_add(chunk.len() as u64, Ordering::Relaxed);

        let mut count = 0;
        while let Ok(Some(frame)) = ProtocolDecoder::decode(buffer) {
            count += 1;
            self.stats.frames_processed.fetch_add(1, Ordering::Relaxed);
            if self.sender.send(frame).await.is_err() {
                break;
            }
        }
        Ok(count)
    }
}
