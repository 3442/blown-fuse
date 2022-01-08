use crate::{proto, sealed::Sealed};
use std::marker::PhantomData;

pub mod io;
pub mod mount;
pub mod ops;
pub mod session;

pub trait Operation<'o>: Sealed + Sized {
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
