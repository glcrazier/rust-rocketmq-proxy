use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("decode command error: {0}")]
    DecodeCommandError(anyhow::Error),
    #[error("io error")]
    IoError(#[from] std::io::Error),
    #[error("stream not ready")]
    StreamNotReady,
    #[error("invalid address {0}")]
    InvalidAddress(String),
    #[error("request timeout")]
    Timeout,
    #[error("read from server error")]
    ReadError,
    #[error("write to server error: {0}")]
    WriteError(anyhow::Error),
    #[error("not available currently")]
    TryLater,
    #[error("internal error: {0}")]
    InternalError(anyhow::Error)
}

pub fn read_u32(data: &[u8]) -> u32 {
    u32::from_be_bytes([data[0], data[1], data[2], data[3]])
}
