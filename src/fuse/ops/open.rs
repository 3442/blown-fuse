use crate::fuse::{
    io::{AccessFlags, OpenFlags},
    private_trait::Sealed,
    Done, Operation, Reply, Request,
};

use crate::{proto, Errno};
use super::FromRequest;

pub enum Open {}
pub enum Release {}
pub enum Opendir {}
pub enum Releasedir {}
pub enum Access {}

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
    type ReplyTail = ();
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

impl<'o> Reply<'o, Open> {
    pub fn force_direct_io(&mut self) {
        self.tail |= proto::OpenOutFlags::DIRECT_IO;
    }

    pub fn ok(self) -> Done<'o> {
        self.ok_with_handle(0)
    }

    pub fn ok_with_handle(self, handle: u64) -> Done<'o> {
        let open_flags = self.tail.bits();

        self.single(&proto::OpenOut {
            fh: handle,
            open_flags,
            padding: Default::default(),
        })
    }
}

impl<'o> Request<'o, Release> {
    pub fn handle(&self) -> u64 {
        self.body.fh
    }
}

impl<'o> Reply<'o, Release> {
    pub fn ok(self) -> Done<'o> {
        self.empty()
    }
}

impl<'o> Reply<'o, Opendir> {
    pub fn ok(self) -> Done<'o> {
        self.ok_with_handle(0)
    }

    pub fn ok_with_handle(self, handle: u64) -> Done<'o> {
        self.single(&proto::OpenOut {
            fh: handle,
            open_flags: 0,
            padding: Default::default(),
        })
    }
}

impl<'o> Request<'o, Releasedir> {
    pub fn handle(&self) -> u64 {
        self.body.release_in.fh
    }
}

impl<'o> Reply<'o, Releasedir> {
    pub fn ok(self) -> Done<'o> {
        self.empty()
    }
}

impl<'o> Request<'o, Access> {
    pub fn mask(&self) -> AccessFlags {
        AccessFlags::from_bits_truncate(self.body.mask as i32)
    }
}

impl<'o> Reply<'o, Access> {
    pub fn ok(self) -> Done<'o> {
        self.empty()
    }

    pub fn permission_denied(self) -> Done<'o> {
        self.fail(Errno::EACCES)
    }
}

impl<'o> FromRequest<'o, Open> for proto::OpenOutFlags {
    fn from_request(_request: &Request<'o, Open>) -> Self {
        proto::OpenOutFlags::empty()
    }
}
