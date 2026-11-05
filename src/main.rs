use clap::Parser;
use std::time::Instant;
use stream_buffer_engine::{EngineConfig, ProtocolDecoder, RingBuffer, StreamEngine};

#[derive(Parser, Debug)]
#[command(author = "Lazarus Ebi", version = "0.1.0", about = "High-Throughput Stream Engine")]
struct Args {
    #[arg(short, long, default_value_t = 50_000)]
    frames: usize,

    #[arg(short, long, default_value_t = 256)]
    payload_size: usize,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("🚀 Starting Stream Engine Benchmark: {} frames", args.frames);

    let config = EngineConfig::default();
    let (engine, mut rx, stats) = StreamEngine::new(config);

    let consumer = tokio::spawn(async move {
        let mut count = 0;
        while let Some(_) = rx.recv().await {
            count += 1;
        }
        count
    });

    let sample = vec![0xAB; args.payload_size];
    let wire = ProtocolDecoder::encode(1, &sample);
    let mut ring = RingBuffer::new(1024 * 1024 * 8);

    let start = Instant::now();
    for _ in 0..args.frames {
        engine.ingest_chunk(&mut ring, &wire).await.unwrap();
    }
    drop(engine);

    let consumed = consumer.await.unwrap();
    let elapsed = start.elapsed();
    println!("✅ Benchmark finished: {} frames in {:.3?}", consumed, elapsed);
}
