use quick_error::quick_error;

quick_error! {
    #[derive(Debug)]
    pub enum FuseError {
        Io(err: std::io::Error) { from() }
        ProtocolInit { display("fuse handshake failed (ancient kernel?)") }
        Truncated { display("fuse request truncated") }
        BadOpcode { display("unknown fuse operation") }
        BadLength { display("bad length in fuse request") }
        ShortWrite { display("fuse reply was trimmed on write()") }
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum MountError {
        Io(err: std::io::Error) { from() }
        Fusermount { display("fusermount failed") }
    }
}

pub type FuseResult<T> = Result<T, FuseError>;
