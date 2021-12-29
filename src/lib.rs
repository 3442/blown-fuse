//! An asynchronous and high-level implementation of the Filesystem in Userspace protocol.
//!
//! `blown-fuse`

#![forbid(unsafe_code)]
#![feature(try_trait_v2, doc_cfg)]

#[cfg(not(target_os = "linux"))]
compile_error!("Unsupported OS");

use std::time::{SystemTime, UNIX_EPOCH};

pub use nix;

#[cfg(any(feature = "server", doc))]
#[doc(cfg(feature = "server"))]
pub use crate::fuse::*;

#[cfg(any(feature = "client", doc, test))]
#[doc(cfg(feature = "client"))]
pub mod client;

mod proto;
mod util;

#[cfg(any(feature = "server", doc))]
#[doc(cfg(feature = "server"))]
mod fuse;

#[doc(no_inline)]
pub use nix::errno::Errno;

pub use util::{FuseError, FuseResult};

#[derive(Copy, Clone, Default, Eq, PartialEq)]
pub struct Ttl {
    seconds: u64,
    nanoseconds: u32,
}

#[derive(Copy, Clone, Default, Eq, PartialEq)]
pub struct Timestamp {
    seconds: u64,
    nanoseconds: u32,
}

/// Inode number.
///
/// This is a public newtype. Users are expected to inspect the underlying `u64` and construct
/// arbitrary `Ino` objects.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Ino(pub u64);

impl Ino {
    /// The invalid inode number, mostly useful for internal aspects of the FUSE protocol.
    pub const NULL: Self = Ino(0);

    /// The inode number of the root inode as observed by a FUSE client. Other libraries refer to
    /// this as `FUSE_ROOT_ID`.
    ///
    /// Note that a mounted session will remember the inode number given by `Inode::ino()` for the
    /// root inode at initialization and transparently swap between it and `Ino::ROOT`. During
    /// dispatch, requests targeted at `Ino::ROOT` will have this value replaced by the stored root
    /// number, while replies involving the root inode will always report `Ino::ROOT` to the FUSE
    /// client. Therefore, filesystems do not have to be aware of `Ino::ROOT` in most cases.
    pub const ROOT: Self = Ino(proto::ROOT_ID);
}

impl std::fmt::Display for Ino {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl Ttl {
    pub const MAX: Self = Ttl {
        seconds: u64::MAX,
        nanoseconds: u32::MAX,
    };

    pub fn new(seconds: u64, nanoseconds: u32) -> Ttl {
        assert!(nanoseconds < 1_000_000_000);
        Ttl {
            seconds,
            nanoseconds,
        }
    }

    pub fn seconds(&self) -> u64 {
        self.seconds
    }

    pub fn nanoseconds(&self) -> u32 {
        self.nanoseconds
    }
}

impl From<SystemTime> for Timestamp {
    fn from(time: SystemTime) -> Self {
        let duration = time.duration_since(UNIX_EPOCH).unwrap();
        Timestamp {
            seconds: duration.as_secs(),
            nanoseconds: duration.subsec_nanos(),
        }
    }
}
