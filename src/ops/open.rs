use crate::{
    io::{AccessFlags, OpenFlags},
    private_trait::Sealed,
    proto, Done, Errno, Operation, Reply, Request,
};

use super::{traits::ReplyOk, FromRequest};

pub enum Open {}
pub enum Release {}
pub enum Opendir {}
pub enum Releasedir {}
pub enum Access {}

pub trait ReplyOpen<'o>: ReplyOk<'o, ReplyTail = proto::OpenOutFlags> {
    fn ok_with_handle(reply: Reply<'o, Self>, handle: u64) -> Done<'o> {
        let open_flags = reply.tail.bits();

        reply.single(&proto::OpenOut {
            fh: handle,
            open_flags,
            padding: Default::default(),
        })
    }

    fn force_direct_io(reply: &mut Reply<'o, Self>) {
        reply.tail |= proto::OpenOutFlags::DIRECT_IO;
    }
}

pub trait ReplyPermissionDenied<'o>: Operation<'o> {
    fn permission_denied(reply: Reply<'o, Self>) -> Done<'o> {
        reply.fail(Errno::EACCES)
    }
}

impl Sealed for Open {}
impl Sealed for Release {}
impl Sealed for Opendir {}
impl Sealed for Releasedir {}
impl Sealed for Access {}

impl<'o> Operation<'o> for Open {
    type RequestBody = &'o proto::OpenIn;
    type ReplyTail = proto::OpenOutFlags;
}

impl<'o> Operation<'o> for Release {
    type RequestBody = &'o proto::ReleaseIn;
    type ReplyTail = ();
}

impl<'o> Operation<'o> for Opendir {
    type RequestBody = &'o proto::OpendirIn;
    type ReplyTail = proto::OpenOutFlags;
}

impl<'o> Operation<'o> for Releasedir {
    type RequestBody = &'o proto::ReleasedirIn;
    type ReplyTail = ();
}

impl<'o> Operation<'o> for Access {
    type RequestBody = &'o proto::AccessIn;
    type ReplyTail = ();
}

impl<'o> Request<'o, Open> {
    pub fn flags(&self) -> OpenFlags {
        OpenFlags::from_bits_truncate(self.body.flags.try_into().unwrap_or_default())
    }
}

impl<'o> ReplyOk<'o> for Open {
    fn ok(reply: Reply<'o, Self>) -> Done<'o> {
        reply.ok_with_handle(0)
    }
}

impl<'o> ReplyOpen<'o> for Open {}
impl<'o> ReplyPermissionDenied<'o> for Open {}

impl<'o> Request<'o, Release> {
    pub fn handle(&self) -> u64 {
        self.body.fh
    }
}

impl<'o> ReplyOk<'o> for Release {}

impl<'o> ReplyOk<'o> for Opendir {
    fn ok(reply: Reply<'o, Self>) -> Done<'o> {
        reply.ok_with_handle(0)
    }
}

impl<'o> ReplyPermissionDenied<'o> for Opendir {}
impl<'o> ReplyOpen<'o> for Opendir {}

impl<'o> Request<'o, Releasedir> {
    pub fn handle(&self) -> u64 {
        self.body.release_in.fh
    }
}

impl<'o> ReplyOk<'o> for Releasedir {}

impl<'o> Request<'o, Access> {
    pub fn mask(&self) -> AccessFlags {
        AccessFlags::from_bits_truncate(self.body.mask as i32)
    }
}

impl<'o> ReplyOk<'o> for Access {}

impl<'o> ReplyPermissionDenied<'o> for Access {}

impl<'o, O: ReplyOpen<'o>> FromRequest<'o, O> for proto::OpenOutFlags {
    fn from_request(_request: &Request<'o, O>) -> Self {
        proto::OpenOutFlags::empty()
    }
}
