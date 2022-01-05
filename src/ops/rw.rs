use super::{
    traits::{ReplyGather, ReplyOk},
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
    type ReplyTail = ();
}

impl<'o> Operation<'o> for Read {
    type RequestBody = &'o proto::ReadIn;
    type ReplyTail = ();
}

impl<'o> Operation<'o> for Write {
    type RequestBody = (&'o proto::WriteIn, &'o [u8]);
    type ReplyTail = WriteState;
}

impl<'o> Operation<'o> for Flush {
    type RequestBody = &'o proto::FlushIn;
    type ReplyTail = ();
}

impl<'o> ReplyGather<'o> for Readlink {}

impl<'o> Request<'o, Read> {
    pub fn handle(&self) -> u64 {
        self.body.fh
    }

    pub fn offset(&self) -> u64 {
        self.body.offset
    }

    pub fn size(&self) -> u32 {
        self.body.size
    }
}

impl<'o> ReplyGather<'o> for Read {}

impl<'o> Request<'o, Write> {
    pub fn handle(&self) -> u64 {
        self.body.0.fh
    }

    pub fn offset(&self) -> u64 {
        self.body.0.offset
    }

    pub fn data(&self) -> &[u8] {
        self.body.1
    }
}

impl<'o> ReplyAll<'o> for Write {
    fn all(reply: Reply<'o, Self>) -> Done<'o> {
        let size = reply.tail.size;
        reply.single(&proto::WriteOut {
            size,
            padding: Default::default(),
        })
    }
}

impl<'o> Request<'o, Flush> {
    pub fn handle(&self) -> u64 {
        self.body.fh
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
