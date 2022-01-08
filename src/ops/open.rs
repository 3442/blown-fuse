use crate::{
    io::{AccessFlags, Known, Mode, OpenFlags, Stat, Ttl},
    proto::{self, OpenOutFlags},
    sealed::Sealed,
    util::OutputChain,
    Done, Errno, Operation, Reply, Request,
};

use super::{
    c_to_os, make_entry,
    traits::{ReplyKnown, ReplyOk, RequestFlags, RequestHandle, RequestMode, RequestName},
    FromRequest,
};

use bytemuck::bytes_of;
use std::ffi::{CStr, OsStr};

pub enum Open {}
pub enum Release {}
pub enum Opendir {}
pub enum Releasedir {}
pub enum Access {}
pub enum Create {}

pub trait ReplyOpen<'o>: Operation<'o, ReplyState = OpenOutFlags> {
    fn ok_with_handle(reply: Reply<'o, Self>, handle: u64) -> Done<'o>
    where
        Self: ReplyOk<'o>,
    {
        let open_flags = open_flags_bits(reply.state);

        reply.single(&proto::OpenOut {
            fh: handle,
            open_flags,
            padding: Default::default(),
        })
    }

    fn known_with_handle(
        reply: Reply<'o, Self>,
        known: impl Known,
        ttl: Ttl,
        handle: u64,
    ) -> Done<'o>
    where
        Self: ReplyKnown<'o>,
    {
        let (attrs, attrs_ttl) = known.inode().attrs();
        let attrs = attrs.finish(known.inode());

        let entry = make_entry((known.inode().ino(), ttl), (attrs, attrs_ttl));
        let open = proto::OpenOut {
            fh: handle,
            open_flags: open_flags_bits(reply.state),
            padding: Default::default(),
        };

        let done = reply.chain(OutputChain::tail(&[bytes_of(&entry), bytes_of(&open)]));
        known.unveil();

        done
    }

    fn force_direct_io(reply: &mut Reply<'o, Self>) {
        reply.state |= OpenOutFlags::DIRECT_IO;
    }

    fn non_seekable(reply: &mut Reply<'o, Self>) {
        reply.state |= OpenOutFlags::NONSEEKABLE;
    }

    fn is_stream(reply: &mut Reply<'o, Self>) {
        reply.state |= OpenOutFlags::STREAM;
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
impl Sealed for Create {}

impl<'o> Operation<'o> for Open {
    type RequestBody = &'o proto::OpenIn;
    type ReplyState = OpenOutFlags;
}

impl<'o> Operation<'o> for Release {
    type RequestBody = &'o proto::ReleaseIn;
    type ReplyState = ();
}

impl<'o> Operation<'o> for Opendir {
    type RequestBody = &'o proto::OpendirIn;
    type ReplyState = OpenOutFlags;
}

impl<'o> Operation<'o> for Releasedir {
    type RequestBody = &'o proto::ReleasedirIn;
    type ReplyState = ();
}

impl<'o> Operation<'o> for Access {
    type RequestBody = &'o proto::AccessIn;
    type ReplyState = ();
}

impl<'o> Operation<'o> for Create {
    type RequestBody = (&'o proto::CreateIn, &'o CStr);
    type ReplyState = OpenOutFlags;
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

impl<'o, O: ReplyOpen<'o>> FromRequest<'o, O> for OpenOutFlags {
    fn from_request(_request: &Request<'o, O>) -> Self {
        OpenOutFlags::empty()
    }
}

impl<'o> RequestName<'o> for Create {
    fn name<'a>(request: &'a Request<'o, Self>) -> &'a OsStr {
        let (_header, name) = request.body;
        c_to_os(name)
    }
}

impl<'o> RequestMode<'o> for Create {
    fn mode(request: &Request<'o, Self>) -> Mode {
        let (header, _name) = request.body;
        Mode::from_bits_truncate(header.mode)
    }

    fn umask(request: &Request<'o, Self>) -> Mode {
        let (header, _name) = request.body;
        Mode::from_bits_truncate(header.umask)
    }
}

impl<'o> RequestFlags<'o> for Create {
    type Flags = OpenFlags;

    fn flags(request: &Request<'o, Self>) -> Self::Flags {
        let (header, _name) = request.body;
        OpenFlags::from_bits_truncate(header.flags as _)
    }
}

impl<'o> ReplyKnown<'o> for Create {
    fn known(reply: Reply<'o, Self>, entry: impl Known, ttl: Ttl) -> Done<'o> {
        reply.known_with_handle(entry, ttl, 0)
    }
}

impl<'o> ReplyOpen<'o> for Create {}
impl<'o> ReplyPermissionDenied<'o> for Create {}

fn open_flags_bits(flags: OpenOutFlags) -> u32 {
    (flags & OpenOutFlags::KEEP_CACHE & OpenOutFlags::CACHE_DIR).bits()
}
