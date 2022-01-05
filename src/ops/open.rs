use crate::{
    io::{AccessFlags, OpenFlags},
    private_trait::Sealed,
    proto, Done, Errno, Operation, Reply, Request,
};

use super::{
    traits::{ReplyOk, RequestFlags, RequestHandle},
    FromRequest,
};

pub enum Open {}
pub enum Release {}
pub enum Opendir {}
pub enum Releasedir {}
pub enum Access {}

pub trait ReplyOpen<'o>: ReplyOk<'o, ReplyState = proto::OpenOutFlags> {
    fn ok_with_handle(reply: Reply<'o, Self>, handle: u64) -> Done<'o> {
        let open_flags = reply.state.bits();

        reply.single(&proto::OpenOut {
            fh: handle,
            open_flags,
            padding: Default::default(),
        })
    }

    fn force_direct_io(reply: &mut Reply<'o, Self>) {
        reply.state |= proto::OpenOutFlags::DIRECT_IO;
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
    type ReplyState = proto::OpenOutFlags;
}

impl<'o> Operation<'o> for Release {
    type RequestBody = &'o proto::ReleaseIn;
    type ReplyState = ();
}

impl<'o> Operation<'o> for Opendir {
    type RequestBody = &'o proto::OpendirIn;
    type ReplyState = proto::OpenOutFlags;
}

impl<'o> Operation<'o> for Releasedir {
    type RequestBody = &'o proto::ReleasedirIn;
    type ReplyState = ();
}

impl<'o> Operation<'o> for Access {
    type RequestBody = &'o proto::AccessIn;
    type ReplyState = ();
}

impl<'o> RequestFlags<'o> for Open {
    type Flags = OpenFlags;

    fn flags(request: &Request<'o, Self>) -> Self::Flags {
        OpenFlags::from_bits_truncate(request.body.flags as _)
    }
}

impl<'o> ReplyOk<'o> for Open {
    fn ok(reply: Reply<'o, Self>) -> Done<'o> {
        reply.ok_with_handle(0)
    }
}

impl<'o> ReplyOpen<'o> for Open {}
impl<'o> ReplyPermissionDenied<'o> for Open {}

impl<'o> RequestHandle<'o> for Release {
    fn handle(request: &Request<'o, Self>) -> u64 {
        request.body.fh
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

impl<'o> RequestHandle<'o> for Releasedir {
    fn handle(request: &Request<'o, Self>) -> u64 {
        request.body.release_in.fh
    }
}

impl<'o> ReplyOk<'o> for Releasedir {}

impl<'o> RequestFlags<'o> for Access {
    type Flags = AccessFlags;

    fn flags(request: &Request<'o, Self>) -> Self::Flags {
        AccessFlags::from_bits_truncate(request.body.mask as i32)
    }
}

impl<'o> ReplyOk<'o> for Access {}

impl<'o> ReplyPermissionDenied<'o> for Access {}

impl<'o, O: ReplyOpen<'o>> FromRequest<'o, O> for proto::OpenOutFlags {
    fn from_request(_request: &Request<'o, O>) -> Self {
        proto::OpenOutFlags::empty()
    }
}
