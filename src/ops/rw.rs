use super::FromRequest;
use crate::{private_trait::Sealed, proto, util::OutputChain, Done, Operation, Reply, Request};
use std::{ffi::OsStr, os::unix::ffi::OsStrExt};

pub enum Readlink {}
pub enum Read {}
pub enum Write {}
pub enum Flush {}

pub struct WriteState {
    size: u32,
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

impl<'o> Reply<'o, Readlink> {
    /// This inode corresponds to a symbolic link pointing to the given target path.
    pub fn target<T: AsRef<OsStr>>(self, target: T) -> Done<'o> {
        self.chain(OutputChain::tail(&[target.as_ref().as_bytes()]))
    }

    /// Same as [`Reply::target()`], except that the target path is taken from disjoint
    /// slices. This involves no additional allocation.
    pub fn gather_target(self, target: &[&[u8]]) -> Done<'o> {
        self.chain(OutputChain::tail(target))
    }
}

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

impl<'o> Reply<'o, Read> {
    pub fn slice(self, data: &[u8]) -> Done<'o> {
        self.chain(OutputChain::tail(&[data]))
    }
}

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

impl<'o> Reply<'o, Write> {
    pub fn all(self) -> Done<'o> {
        let size = self.tail.size;
        self.single(&proto::WriteOut {
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

impl<'o> Reply<'o, Flush> {
    pub fn ok(self) -> Done<'o> {
        self.empty()
    }
}

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
