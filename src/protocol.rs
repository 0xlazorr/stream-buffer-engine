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

/// Explicit Finite State Machine for network stream protocol decoding
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecoderState {
    AwaitingHeader { needed: usize },
    ValidatingHeader { magic: u16 },
    AwaitingPayload { stream_id: u16, payload_len: usize, expected_checksum: u16, needed: usize },
    VerifyingChecksum { stream_id: u16, payload_len: usize },
    FrameComplete,
    ErrorRecovered,
}

pub struct FrameDecoderStateMachine {
    state: DecoderState,
}

impl Default for FrameDecoderStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameDecoderStateMachine {
    pub fn new() -> Self {
        Self {
            state: DecoderState::AwaitingHeader { needed: HEADER_SIZE },
        }
    }

    pub fn state(&self) -> &DecoderState {
        &self.state
    }

    pub fn decode_step(&mut self, buf: &mut crate::buffer::RingBuffer) -> Result<Option<Frame>, ProtocolError> {
        if buf.len() < HEADER_SIZE {
            self.state = DecoderState::AwaitingHeader { needed: HEADER_SIZE - buf.len() };
            return Ok(None);
        }

        let slice = buf.as_slice();
        let magic = u16::from_be_bytes([slice[0], slice[1]]);
        self.state = DecoderState::ValidatingHeader { magic };

        if magic != MAGIC_BYTE {
            buf.advance(1);
            self.state = DecoderState::ErrorRecovered;
            return Err(ProtocolError::InvalidMagic(magic));
        }

        let stream_id = u16::from_be_bytes([slice[2], slice[3]]);
        let payload_len = u32::from_be_bytes([slice[4], slice[5], slice[6], slice[7]]) as usize;
        let expected_checksum = u16::from_be_bytes([slice[8], slice[9]]);

        let total_frame_len = HEADER_SIZE + payload_len;
        if buf.len() < total_frame_len {
            self.state = DecoderState::AwaitingPayload {
                stream_id,
                payload_len,
                expected_checksum,
                needed: total_frame_len - buf.len(),
            };
            return Ok(None);
        }

        self.state = DecoderState::VerifyingChecksum { stream_id, payload_len };
        let payload_slice = &buf.as_slice()[HEADER_SIZE..total_frame_len];
        let mut hasher = Hasher::new();
        hasher.update(payload_slice);
        let computed_checksum = (hasher.finalize() & 0xFFFF) as u16;

        if computed_checksum != expected_checksum {
            // Discard the entire invalid frame to prevent leaking corrupted payload into header parsing
            buf.advance(total_frame_len);
            self.state = DecoderState::ErrorRecovered;
            return Err(ProtocolError::ChecksumMismatch {
                expected: expected_checksum,
                computed: computed_checksum,
            });
        }

        let payload = Bytes::copy_from_slice(payload_slice);
        buf.advance(total_frame_len);
        self.state = DecoderState::FrameComplete;

        Ok(Some(Frame { stream_id, payload }))
    }
}

pub struct ProtocolDecoder;

impl ProtocolDecoder {
    pub fn decode(buf: &mut crate::buffer::RingBuffer) -> Result<Option<Frame>, ProtocolError> {
        let mut fsm = FrameDecoderStateMachine::new();
        fsm.decode_step(buf)
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
