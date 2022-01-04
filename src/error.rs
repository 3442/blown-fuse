use thiserror::Error;

pub type FuseResult<T> = Result<T, FuseError>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FuseError {
    #[error("I/O error")]
    Io(#[from] std::io::Error),

    #[error("fuse handshake failed (ancient kernel?)")]
    ProtocolInit,

    #[error("fuse request truncated")]
    Truncated,

    #[error("unknown fuse operation")]
    BadOpcode,

    #[error("bad length in fuse request")]
    BadLength,

    #[error("fuse reply was trimmed on write()")]
    ShortWrite,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MountError {
    #[error("I/O error during mount")]
    Io(#[from] std::io::Error),

    #[error("fusermount failed")]
    Fusermount,
}
