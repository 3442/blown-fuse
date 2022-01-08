//! An asynchronous and high-level implementation of the Filesystem in Userspace protocol.
//!
//! `blown-fuse`

#![forbid(unsafe_code)]
#![feature(try_trait_v2)]

#[cfg(not(target_os = "linux"))]
compile_error!("Unsupported OS");

use std::marker::PhantomData;

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

#[must_use]
pub struct Done<'o>(PhantomData<&'o mut &'o ()>);

impl Done<'_> {
    fn new() -> Self {
        Done(PhantomData)
    }

    fn consume(self) {
        drop(self);
    }
}

mod sealed {
    pub trait Sealed {}
}
