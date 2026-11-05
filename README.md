# Stream Buffer Engine ⚡🦀

A production-grade, zero-copy network stream processing daemon and protocol buffer parser built in **Rust** with **Tokio** and **Bytes**.

Engineered for microsecond-latency data ingestion, lock-free ring buffering, length-prefixed protocol framing, and CRC32 verification.

---

## 🌟 Key Architecture & Highlights

- **Zero-Copy Ring Buffering**: Reuses underlying byte slices with `bytes::BytesMut`, eliminating allocation thrash during streaming.
- **Framed Binary Protocol**: Implements a compact 10-byte header with magic marker (`0xAA55`), stream demultiplexing, payload length, and CRC32 checksums.
- **Asynchronous Tokio Pipeline**: Decouples network I/O from consumer workers via bounded actor channels (`tokio::sync::mpsc`).
- **Benchmark Performance**: Designed to process over **500,000+ frames/sec** with predictable memory bounds.

---

## 📂 Project Structure

```text
stream-buffer-engine/
├── Cargo.toml             # Rust manifest & dependencies
├── src/
│   ├── lib.rs             # Public API exports
│   ├── buffer.rs          # Circular ring buffer abstraction
│   ├── protocol.rs        # Wire framing, zero-copy decoder, CRC32
│   ├── engine.rs          # Asynchronous stream orchestration & stats
│   └── main.rs            # CLI benchmark runner
└── README.md              # Documentation
```

---

## 🚀 Quickstart

### Build & Run Benchmark:
```bash
cargo build --release
cargo run --release -- --frames 200000 --payload-size 512
```

---

## 📜 License
MIT License • Built by [Lazarus Ebi](https://github.com/0xlazorr)
