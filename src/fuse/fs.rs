use crate::{Ino, TimeToLive};
use async_trait::async_trait;
use std::{num::NonZeroUsize, ops::Deref, sync::Arc};

use super::{
    io::{Attrs, EntryType},
    ops::*,
    Done, Op, Reply,
};

#[async_trait]
pub trait Fuse: Sized + Send + Sync + 'static {
    type Inode: Inode<Fuse = Self> + ?Sized;
    type Farc: Deref<Target = Self::Inode> + Clone + Send + Sync = Arc<Self::Inode>;

    async fn init<'o>(&self, reply: Reply<'o, Self, Init>) -> Done<'o>;

    async fn statfs<'o>(&self, (_, reply, _): Op<'o, Self, Statfs>) -> Done<'o> {
        reply.not_implemented()
    }

    fn request_buffers(&self) -> NonZeroUsize {
        NonZeroUsize::new(16).unwrap()
    }

    fn request_buffer_pages(&self) -> NonZeroUsize {
        NonZeroUsize::new(4).unwrap()
    }
}

#[async_trait]
pub trait Inode: Send + Sync {
    type Fuse: Fuse<Inode = Self>;

    fn ino(self: &FarcTo<Self>) -> Ino;
    fn attrs(self: &FarcTo<Self>) -> (Attrs, TimeToLive);
    fn inode_type(self: &FarcTo<Self>) -> EntryType;

    fn direct_io(self: &FarcTo<Self>) -> bool {
        false
    }

    fn access<'o>(self: &FarcTo<Self>, (_, reply, _): Op<'o, Self::Fuse, Access>) -> Done<'o> {
        reply.not_implemented()
    }

    async fn lookup<'o>(self: FarcTo<Self>, (_, reply, _): Op<'o, Self::Fuse, Lookup>) -> Done<'o> {
        reply.not_implemented()
    }

    async fn readlink<'o>(
        self: FarcTo<Self>,
        (_, reply, _): Op<'o, Self::Fuse, Readlink>,
    ) -> Done<'o> {
        reply.not_implemented()
    }

    async fn open<'o>(self: FarcTo<Self>, (_, reply, _): Op<'o, Self::Fuse, Open>) -> Done<'o> {
        // Calling not_implemented() here would ignore direct_io() and similar flags
        reply.ok()
    }

    async fn opendir<'o>(
        self: FarcTo<Self>,
        (_, reply, _): Op<'o, Self::Fuse, Opendir>,
    ) -> Done<'o> {
        reply.not_implemented()
    }

    async fn readdir<'o>(
        self: FarcTo<Self>,
        (_, reply, _): Op<'o, Self::Fuse, Readdir>,
    ) -> Done<'o> {
        reply.not_implemented()
    }
}

pub type FarcTo<I> = <<I as Inode>::Fuse as Fuse>::Farc;
