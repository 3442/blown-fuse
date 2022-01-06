use crate::{
    io::{Entry, FsInfo, Interruptible, Known, Mode, Stat},
    Done, Ino, Operation, Reply, Request, Ttl,
};

pub use super::{
    dir::{ReplyEntries, ReplyFound},
    entry::{ReplyStat, RequestForget, RequestLink, RequestTarget},
    global::ReplyFsInfo,
    open::{ReplyOpen, ReplyPermissionDenied},
    rw::ReplyAll,
    xattr::ReplyXattrRead,
};

use super::make_entry;
use bytes::BufMut;
use std::{ffi::OsStr, os::unix::ffi::OsStrExt};

pub trait RequestName<'o>: Operation<'o> {
    fn name<'a>(request: &'a Request<'o, Self>) -> &'a OsStr;
}

pub trait RequestSize<'o>: Operation<'o> {
    fn size(request: &Request<'o, Self>) -> u32;
}

pub trait RequestOffset<'o>: Operation<'o> {
    fn offset(request: &Request<'o, Self>) -> u64;
}

pub trait RequestHandle<'o>: Operation<'o> {
    fn handle(request: &Request<'o, Self>) -> u64;
}

pub trait RequestData<'o>: Operation<'o> {
    fn data<'a>(request: &'a Request<'o, Self>) -> &'a [u8];
}

pub trait RequestFlags<'o>: Operation<'o> {
    type Flags: Copy;
    fn flags(request: &Request<'o, Self>) -> Self::Flags;
}

pub trait RequestMode<'o>: Operation<'o> {
    fn mode(request: &Request<'o, Self>) -> Mode;
}

pub trait ReplyOk<'o>: Operation<'o> {
    fn ok(reply: Reply<'o, Self>) -> Done<'o> {
        reply.empty()
    }
}

pub trait ReplyKnown<'o>: Operation<'o> {
    fn known(reply: Reply<'o, Self>, entry: impl Known, ttl: Ttl) -> Done<'o> {
        let (attrs, attrs_ttl) = entry.inode().attrs();
        let attrs = attrs.finish(entry.inode());

        let done = reply.single(&make_entry((entry.inode().ino(), ttl), (attrs, attrs_ttl)));
        entry.unveil();

        done
    }
}

pub trait ReplyNotFound<'o>: Operation<'o> {
    fn not_found(reply: Reply<'o, Self>) -> Done<'o>;
}

pub trait ReplyBuffered<'o, B>: Operation<'o>
where
    B: BufMut + AsRef<[u8]>,
{
    type Buffered: Operation<'o>;
    fn buffered(reply: Reply<'o, Self>, buffer: B) -> Reply<'o, Self::Buffered>;
}

pub trait ReplyGather<'o>: Operation<'o> {
    fn blob(reply: Reply<'o, Self>, blob: impl AsRef<OsStr>) -> Done<'o> {
        Self::slice(reply, blob.as_ref().as_bytes())
    }

    fn slice(reply: Reply<'o, Self>, slice: impl AsRef<[u8]>) -> Done<'o> {
        Self::gather(reply, &[slice.as_ref()])
    }

    fn gather(reply: Reply<'o, Self>, fragments: &[&[u8]]) -> Done<'o> {
        reply.chain(crate::util::OutputChain::tail(fragments))
    }
}

impl<'o, O: Operation<'o>> Request<'o, O> {
    pub fn name(&self) -> &OsStr
    where
        O: RequestName<'o>,
    {
        O::name(self)
    }

    pub fn size(&self) -> u32
    where
        O: RequestSize<'o>,
    {
        O::size(self)
    }

    pub fn offset(&self) -> u64
    where
        O: RequestOffset<'o>,
    {
        O::offset(self)
    }

    pub fn handle(&self) -> u64
    where
        O: RequestHandle<'o>,
    {
        O::handle(self)
    }

    pub fn data(&self) -> &[u8]
    where
        O: RequestData<'o>,
    {
        O::data(self)
    }

    pub fn flags(&self) -> O::Flags
    where
        O: RequestFlags<'o>,
    {
        O::flags(self)
    }

    pub fn mode(&self) -> Mode
    where
        O: RequestMode<'o>,
    {
        O::mode(self)
    }

    pub fn forget_list(&self) -> impl '_ + Iterator<Item = (Ino, u64)>
    where
        O: RequestForget<'o>,
    {
        O::forget_list(self)
    }

    pub fn target(&self) -> &OsStr
    where
        O: RequestTarget<'o>,
    {
        O::target(self)
    }

    pub fn source_ino(&self) -> Ino
    where
        O: RequestLink<'o>,
    {
        O::source_ino(self)
    }
}

impl<'o, O: Operation<'o>> Reply<'o, O> {
    pub fn ok(self) -> Done<'o>
    where
        O: ReplyOk<'o>,
    {
        O::ok(self)
    }

    pub fn known(self, entry: impl Known, ttl: Ttl) -> Done<'o>
    where
        O: ReplyKnown<'o>,
    {
        O::known(self, entry, ttl)
    }

    pub fn not_found(self) -> Done<'o>
    where
        O: ReplyNotFound<'o>,
    {
        O::not_found(self)
    }

    pub fn permission_denied(self) -> Done<'o>
    where
        O: ReplyPermissionDenied<'o>,
    {
        O::permission_denied(self)
    }

    pub fn stat(self, inode: &impl Stat) -> Done<'o>
    where
        O: ReplyStat<'o>,
    {
        O::stat(self, inode)
    }

    pub fn ok_with_handle(self, handle: u64) -> Done<'o>
    where
        O: ReplyOpen<'o>,
    {
        O::ok_with_handle(self, handle)
    }

    pub fn force_direct_io(&mut self)
    where
        O: ReplyOpen<'o>,
    {
        O::force_direct_io(self)
    }

    pub fn not_found_for(self, ttl: Ttl) -> Done<'o>
    where
        O: ReplyFound<'o>,
    {
        O::not_found_for(self, ttl)
    }

    pub fn entry(self, entry: Entry<impl Known>) -> Interruptible<'o, O, ()>
    where
        O: ReplyEntries<'o>,
    {
        O::entry(self, entry)
    }

    pub fn end(self) -> Done<'o>
    where
        O: ReplyEntries<'o>,
    {
        O::end(self)
    }

    pub fn all(self) -> Done<'o>
    where
        O: ReplyAll<'o>,
    {
        O::all(self)
    }

    pub fn buffered<B>(self, buffer: B) -> Reply<'o, O::Buffered>
    where
        O: ReplyBuffered<'o, B>,
        B: BufMut + AsRef<[u8]>,
    {
        O::buffered(self, buffer)
    }

    pub fn blob(self, blob: impl AsRef<OsStr>) -> Done<'o>
    where
        O: ReplyGather<'o>,
    {
        O::blob(self, blob)
    }

    pub fn slice(self, slice: impl AsRef<[u8]>) -> Done<'o>
    where
        O: ReplyGather<'o>,
    {
        O::slice(self, slice)
    }

    pub fn gather(self, fragments: &[&[u8]]) -> Done<'o>
    where
        O: ReplyGather<'o>,
    {
        O::gather(self, fragments)
    }

    pub fn info(self, info: &FsInfo) -> Done<'o>
    where
        O: ReplyFsInfo<'o>,
    {
        O::info(self, info)
    }

    pub fn requires_size(self, size: u32) -> Done<'o>
    where
        O: ReplyXattrRead<'o>,
    {
        O::requires_size(self, size)
    }

    pub fn buffer_too_small(self) -> Done<'o>
    where
        O: ReplyXattrRead<'o>,
    {
        O::buffer_too_small(self)
    }
}
