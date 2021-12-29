use crate::proto;
use std::marker::PhantomData;

pub mod io;

#[doc(cfg(feature = "server"))]
pub mod ops;

#[doc(cfg(feature = "mount"))]
pub mod mount;

pub mod session;

mod private_trait {
    pub trait Sealed {}
}

pub trait Operation<'o>: private_trait::Sealed + Sized {
    type RequestBody: crate::proto::Structured<'o>;
    type ReplyTail;
}

pub type Op<'o, O = ops::Any> = (Request<'o, O>, Reply<'o, O>);

#[doc(cfg(feature = "server"))]
pub struct Request<'o, O: Operation<'o>> {
    header: proto::InHeader,
    body: O::RequestBody,
}

#[doc(cfg(feature = "server"))]
#[must_use]
pub struct Reply<'o, O: Operation<'o>> {
    session: &'o session::Session,
    unique: u64,
    tail: O::ReplyTail,
}

#[must_use]
#[doc(cfg(feature = "server"))]
pub struct Done<'o>(PhantomData<*mut &'o ()>);

impl Done<'_> {
    fn done() -> Self {
        Done(PhantomData)
    }
}
