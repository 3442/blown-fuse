use super::{
    traits::{ReplyGather, ReplyOk, RequestData, RequestHandle, RequestOffset, RequestSize},
    FromRequest,
};

use crate::{private_trait::Sealed, proto, Done, Operation, Reply, Request};

pub enum Readlink {}
pub enum Read {}
pub enum Write {}
pub enum Flush {}

pub struct WriteState {
    size: u32,
}

pub trait ReplyAll<'o>: Operation<'o> {
    fn all(reply: Reply<'o, Self>) -> Done<'o>;
}

impl Sealed for Readlink {}
impl Sealed for Read {}
impl Sealed for Write {}
impl Sealed for Flush {}

impl<'o> Operation<'o> for Readlink {
    type RequestBody = ();
    type ReplyState = ();
}

impl<'o> Operation<'o> for Read {
    type RequestBody = &'o proto::ReadIn;
    type ReplyState = ();
}

impl<'o> Operation<'o> for Write {
    type RequestBody = (&'o proto::WriteIn, &'o [u8]);
    type ReplyState = WriteState;
}

impl<'o> Operation<'o> for Flush {
    type RequestBody = &'o proto::FlushIn;
    type ReplyState = ();
}

impl<'o> ReplyGather<'o> for Readlink {}

impl<'o> RequestHandle<'o> for Read {
    fn handle(request: &Request<'o, Self>) -> u64 {
        request.body.fh
    }
}

impl<'o> RequestOffset<'o> for Read {
    fn offset(request: &Request<'o, Self>) -> u64 {
        request.body.offset
    }
}

impl<'o> RequestSize<'o> for Read {
    fn size(request: &Request<'o, Self>) -> u32 {
        request.body.size
    }
}

impl<'o> ReplyGather<'o> for Read {}

impl<'o> RequestHandle<'o> for Write {
    fn handle(request: &Request<'o, Self>) -> u64 {
        request.body.0.fh
    }
}

impl<'o> RequestOffset<'o> for Write {
    fn offset(request: &Request<'o, Self>) -> u64 {
        request.body.0.offset
    }
}

impl<'o> RequestData<'o> for Write {
    fn data<'a>(request: &'a Request<'o, Self>) -> &'a [u8] {
        request.body.1
    }
}

impl<'o> ReplyAll<'o> for Write {
    fn all(reply: Reply<'o, Self>) -> Done<'o> {
        let size = reply.state.size;
        reply.single(&proto::WriteOut {
            size,
            padding: Default::default(),
        })
    }
}

impl<'o> RequestHandle<'o> for Flush {
    fn handle(request: &Request<'o, Self>) -> u64 {
        request.body.fh
    }
}

impl<'o> ReplyOk<'o> for Flush {}

impl<'o> FromRequest<'o, Write> for WriteState {
    fn from_request(request: &Request<'o, Write>) -> Self {
        let (body, data) = request.body;

        if body.size as usize != data.len() {
            log::warn!(
                "Write size={} differs from data.len={}",
                body.size,
                data.len()
            );
        }

        WriteState { size: body.size }
    }
}
