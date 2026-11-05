use bytes::Bytes;
use crc32fast::Hasher;
use thiserror::Error;

pub const MAGIC_BYTE: u16 = 0xAA55;
pub const HEADER_SIZE: usize = 10;

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Incomplete frame header: required {0} bytes")]
    IncompleteHeader(usize),
    #[error("Invalid magic byte prefix: expected 0xAA55, got {0:#06x}")]
    InvalidMagic(u16),
    #[error("Incomplete payload: expected {expected} bytes, available {available}")]
    IncompletePayload { expected: usize, available: usize },
    #[error("Checksum verification failed: expected {expected:#06x}, computed {computed:#06x}")]
    ChecksumMismatch { expected: u16, computed: u16 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub stream_id: u16,
    pub payload: Bytes,
}

pub struct ProtocolDecoder;

impl ProtocolDecoder {
    pub fn decode(buf: &mut crate::buffer::RingBuffer) -> Result<Option<Frame>, ProtocolError> {
        if buf.len() < HEADER_SIZE {
            return Ok(None);
        }

        let slice = buf.as_slice();
        let magic = u16::from_be_bytes([slice[0], slice[1]]);
        if magic != MAGIC_BYTE {
            buf.advance(1);
            return Err(ProtocolError::InvalidMagic(magic));
        }

        let stream_id = u16::from_be_bytes([slice[2], slice[3]]);
        let payload_len = u32::from_be_bytes([slice[4], slice[5], slice[6], slice[7]]) as usize;
        let expected_checksum = u16::from_be_bytes([slice[8], slice[9]]);

        let total_frame_len = HEADER_SIZE + payload_len;
        if buf.len() < total_frame_len {
            return Ok(None);
        }

        let payload_slice = &buf.as_slice()[HEADER_SIZE..total_frame_len];
        let mut hasher = Hasher::new();
        hasher.update(payload_slice);
        let computed_checksum = (hasher.finalize() & 0xFFFF) as u16;

        if computed_checksum != expected_checksum {
            buf.advance(HEADER_SIZE);
            return Err(ProtocolError::ChecksumMismatch {
                expected: expected_checksum,
                computed: computed_checksum,
            });
        }

        let payload = Bytes::copy_from_slice(payload_slice);
        buf.advance(total_frame_len);

        Ok(Some(Frame { stream_id, payload }))
    }

    pub fn encode(stream_id: u16, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_SIZE + payload.len());
        out.extend_from_slice(&MAGIC_BYTE.to_be_bytes());
        out.extend_from_slice(&stream_id.to_be_bytes());
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());

        let mut hasher = Hasher::new();
        hasher.update(payload);
        let checksum = (hasher.finalize() & 0xFFFF) as u16;
        out.extend_from_slice(&checksum.to_be_bytes());
        out.extend_from_slice(payload);
        out
    }
}
