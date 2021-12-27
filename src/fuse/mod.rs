use crate::proto;
use std::marker::PhantomData;

pub mod io;

#[doc(cfg(feature = "server"))]
pub mod ops;

#[doc(cfg(feature = "mount"))]
pub mod mount;

pub mod session;

mod private_trait {
    pub trait Operation<'o> {
        type RequestBody: crate::proto::Structured<'o>;
        type ReplyTail;
    }
}

use private_trait::Operation;

pub type Op<'o, O = ops::Any> = (Request<'o, O>, Reply<'o, O>);

#[doc(cfg(feature = "server"))]
pub struct Request<'o, O: Operation<'o>> {
    header: proto::InHeader,
    body: O::RequestBody,
}

#[doc(cfg(feature = "server"))]
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
