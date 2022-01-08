//! An asynchronous and high-level implementation of the Filesystem in Userspace protocol.
//!
//! `blown-fuse`

#![forbid(unsafe_code)]
#![feature(try_trait_v2)]

#[cfg(not(target_os = "linux"))]
compile_error!("Unsupported OS");

use std::{
    marker::PhantomData,
    time::{SystemTime, UNIX_EPOCH},
};

pub use self::error::{FuseError, FuseResult};

#[doc(no_inline)]
pub use nix::{self, errno::Errno};

pub mod error;
pub mod io;
pub mod mount;
pub mod ops;
pub mod session;

mod proto;
mod util;

pub trait Operation<'o>: sealed::Sealed + Sized {
    type RequestBody: crate::proto::Structured<'o>;
    type ReplyState;
}

pub type Op<'o, O = ops::Any> = (Request<'o, O>, Reply<'o, O>);

pub struct Request<'o, O: Operation<'o>> {
    header: proto::InHeader,
    body: O::RequestBody,
}

#[must_use]
pub struct Reply<'o, O: Operation<'o>> {
    session: &'o session::Session,
    unique: u64,
    state: O::ReplyState,
}

/// Inode number.
///
/// This is a public newtype. Users are expected to inspect the underlying `u64` and construct
/// arbitrary `Ino` objects.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Ino(pub u64);

#[must_use]
pub struct Done<'o>(PhantomData<&'o mut &'o ()>);

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct Ttl {
    seconds: u64,
    nanoseconds: u32,
}

#[derive(Copy, Clone, Default, Eq, PartialEq)]
pub struct Timestamp {
    seconds: i64,
    nanoseconds: u32,
}

impl Done<'_> {
    fn new() -> Self {
        Done(PhantomData)
    }

    fn consume(self) {
        drop(self);
    }
}

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

    /// Extracts the raw inode number.
    pub fn as_raw(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for Ino {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl Ttl {
    pub const NULL: Self = Ttl {
        seconds: 0,
        nanoseconds: 0,
    };

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

impl Timestamp {
    pub fn new(seconds: i64, nanoseconds: u32) -> Self {
        Timestamp {
            seconds,
            nanoseconds,
        }
    }
}

impl From<SystemTime> for Timestamp {
    fn from(time: SystemTime) -> Self {
        let (seconds, nanoseconds) = match time.duration_since(UNIX_EPOCH) {
            Ok(duration) => {
                let secs = duration.as_secs().try_into().unwrap();
                (secs, duration.subsec_nanos())
            }

            Err(before_epoch) => {
                let duration = before_epoch.duration();
                let secs = -i64::try_from(duration.as_secs()).unwrap();
                (secs, duration.subsec_nanos())
            }
        };

        Timestamp {
            seconds,
            nanoseconds,
        }
    }
}

mod sealed {
    pub trait Sealed {}
}
