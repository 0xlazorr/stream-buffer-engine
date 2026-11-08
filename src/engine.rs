use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use crate::buffer::RingBuffer;
use crate::protocol::{Frame, FrameDecoderStateMachine};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineStatus {
    Ready,
    Ingesting,
    Draining,
    Stopped,
}

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
    status: Arc<RwLock<EngineStatus>>,
}

impl StreamEngine {
    pub fn new(config: EngineConfig) -> (Self, mpsc::Receiver<Frame>, Arc<EngineStats>) {
        let (tx, rx) = mpsc::channel(config.worker_channels);
        let stats = Arc::new(EngineStats::default());
        let status = Arc::new(RwLock::new(EngineStatus::Ready));
        let engine = Self {
            config,
            stats: Arc::clone(&stats),
            sender: tx,
            status,
        };
        (engine, rx, stats)
    }

    pub async fn status(&self) -> EngineStatus {
        *self.status.read().await
    }

    pub async fn ingest_chunk(&self, buffer: &mut RingBuffer, chunk: &[u8]) -> Result<usize, &'static str> {
        {
            let mut st = self.status.write().await;
            if *st == EngineStatus::Stopped {
                return Err("Cannot ingest chunk: engine is stopped");
            }
            *st = EngineStatus::Ingesting;
        }

        buffer.extend_from_slice(chunk)?;
        self.stats.bytes_processed.fetch_add(chunk.len() as u64, Ordering::Relaxed);

        let mut count = 0;
        let mut fsm = FrameDecoderStateMachine::new();
        while let Ok(Some(frame)) = fsm.decode_step(buffer) {
            count += 1;
            self.stats.frames_processed.fetch_add(1, Ordering::Relaxed);
            if self.sender.send(frame).await.is_err() {
                let mut st = self.status.write().await;
                *st = EngineStatus::Draining;
                break;
            }
        }

        {
            let mut st = self.status.write().await;
            if *st != EngineStatus::Stopped && *st != EngineStatus::Draining {
                *st = EngineStatus::Ready;
            }
        }

        Ok(count)
    }

    pub async fn stop(&self) {
        let mut st = self.status.write().await;
        *st = EngineStatus::Stopped;
    }
}
