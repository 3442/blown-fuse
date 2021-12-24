use std::{
    collections::HashMap,
    marker::PhantomData,
    os::unix::io::RawFd,
    sync::{Arc, Mutex},
};

use tokio::{
    io::unix::AsyncFd,
    sync::{broadcast, Notify, Semaphore},
};

use crate::{proto, util::DumbFd, FuseResult, Ino};

pub mod io;

#[doc(cfg(feature = "server"))]
pub mod fs;

#[doc(cfg(feature = "server"))]
pub mod ops;

#[doc(cfg(feature = "mount"))]
pub mod mount;

mod session;
use fs::Fuse;

#[doc(cfg(feature = "server"))]
pub struct Session<Fs: Fuse> {
    _fusermount_fd: DumbFd,
    session_fd: AsyncFd<RawFd>,
    proto_minor: u32,
    fs: Fs,
    input_semaphore: Arc<Semaphore>,
    large_buffers: Mutex<Vec<Box<[u8]>>>,
    known: Mutex<HashMap<Ino, (Fs::Farc, u64)>>,
    destroy: Notify,
    interrupt_tx: broadcast::Sender<u64>,
}

#[doc(cfg(feature = "server"))]
pub struct Start {
    fusermount_fd: DumbFd,
    session_fd: DumbFd,
}

mod private_trait {
    pub trait Operation<'o, Fs: super::Fuse> {
        type RequestBody = ();
        type ReplyTail = ();

        fn consume_errno(_errno: i32, _tail: &mut Self::ReplyTail) {}
    }
}

use private_trait::Operation;

#[doc(cfg(feature = "server"))]
pub type Op<'o, Fs, O> = (Request<'o, Fs, O>, Reply<'o, Fs, O>, &'o Arc<Session<Fs>>);

#[doc(cfg(feature = "server"))]
pub struct Request<'o, Fs: Fuse, O: Operation<'o, Fs>> {
    header: &'o proto::InHeader,
    body: O::RequestBody,
}

#[doc(cfg(feature = "server"))]
pub struct Reply<'o, Fs: Fuse, O: Operation<'o, Fs>> {
    session: &'o Session<Fs>,
    unique: u64,
    tail: O::ReplyTail,
}

#[must_use]
#[doc(cfg(feature = "server"))]
pub struct Done<'o>(FuseResult<PhantomData<&'o ()>>);

impl Done<'_> {
    fn from_result(result: FuseResult<()>) -> Self {
        Done(result.map(|()| PhantomData))
    }

    fn into_result(self) -> FuseResult<()> {
        self.0.map(|PhantomData| ())
    }
}
