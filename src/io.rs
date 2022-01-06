use bytemuck::Zeroable;
use nix::{errno::Errno, sys::stat::SFlag};

use std::{
    convert::Infallible,
    ffi::OsStr,
    future::Future,
    ops::{ControlFlow, FromResidual, Try},
};

use super::{Done, Operation, Reply, Request};
use crate::{proto, FuseResult, Ino, Timestamp, Ttl};

#[doc(no_inline)]
pub use nix::{
    dir::Type as EntryType,
    fcntl::OFlag as OpenFlags,
    sys::stat::Mode,
    unistd::{AccessFlags, Gid, Pid, Uid},
};

pub use proto::FsyncFlags;

pub enum Interruptible<'o, O: Operation<'o>, T> {
    Completed(Reply<'o, O>, T),
    Interrupted(Done<'o>),
}

pub trait Stat {
    fn ino(&self) -> Ino;
    fn inode_type(&self) -> EntryType;
    fn attrs(&self) -> (Attrs, Ttl);
}

pub trait Known {
    type Inode: Stat;

    fn inode(&self) -> &Self::Inode;
    fn unveil(self);
}

pub struct Failed<'o, E>(pub Done<'o>, pub E);

pub trait Finish<'o, O: Operation<'o>> {
    fn finish(&self, reply: Reply<'o, O>) -> Done<'o>;
}

#[derive(Clone)]
pub struct Attrs(proto::Attrs);

pub struct Entry<'a, K> {
    pub offset: u64,
    pub name: &'a OsStr,
    pub inode: K,
    pub ttl: Ttl,
}

#[derive(Copy, Clone)]
pub struct FsInfo(proto::StatfsOut);

impl<'o, E> From<Failed<'o, E>> for Done<'o> {
    fn from(failed: Failed<'o, E>) -> Done<'o> {
        failed.0
    }
}

impl<'o, O: Operation<'o>> Finish<'o, O> for Errno {
    fn finish(&self, reply: Reply<'o, O>) -> Done<'o> {
        reply.fail(*self)
    }
}

impl<'o, O: Operation<'o>> Finish<'o, O> for std::io::Error {
    fn finish(&self, reply: Reply<'o, O>) -> Done<'o> {
        reply.fail(
            self.raw_os_error()
                .map(Errno::from_i32)
                .unwrap_or(Errno::EIO),
        )
    }
}

impl<'o, O: Operation<'o>> Request<'o, O> {
    pub fn ino(&self) -> Ino {
        Ino(self.header.ino)
    }

    pub fn generation(&self) -> u64 {
        0
    }

    pub fn uid(&self) -> Uid {
        Uid::from_raw(self.header.uid)
    }

    pub fn gid(&self) -> Gid {
        Gid::from_raw(self.header.gid)
    }

    pub fn pid(&self) -> Pid {
        Pid::from_raw(self.header.pid as i32)
    }
}

impl<'o, O: Operation<'o>> Reply<'o, O> {
    pub async fn interruptible<F, T>(self, f: F) -> Interruptible<'o, O, T>
    where
        F: Future<Output = T>,
    {
        tokio::pin!(f);
        let mut rx = self.session.interrupt_rx();

        use Interruptible::*;
        loop {
            tokio::select! {
                output = &mut f => break Completed(self, output),

                result = rx.recv() => match result {
                    Ok(unique) if unique == self.unique => {
                        break Interrupted(self.interrupted());
                    }

                    _ => continue,
                }
            }
        }
    }

    pub fn and_then<T, E>(self, result: Result<T, E>) -> Result<(Self, T), Failed<'o, E>>
    where
        E: Finish<'o, O>,
    {
        match result {
            Ok(t) => Ok((self, t)),
            Err(error) => {
                let done = error.finish(self);
                Err(Failed(done, error))
            }
        }
    }

    pub fn fail(self, errno: Errno) -> Done<'o> {
        let result = self.session.fail(self.unique, errno as i32);
        self.finish(result)
    }

    pub fn not_implemented(self) -> Done<'o> {
        self.fail(Errno::ENOSYS)
    }

    pub fn not_permitted(self) -> Done<'o> {
        self.fail(Errno::EPERM)
    }

    pub fn io_error(self) -> Done<'o> {
        self.fail(Errno::EIO)
    }

    pub fn invalid_argument(self) -> Done<'o> {
        self.fail(Errno::EINVAL)
    }

    pub fn interrupted(self) -> Done<'o> {
        self.fail(Errno::EINTR)
    }

    pub(crate) fn finish(self, result: FuseResult<()>) -> Done<'o> {
        if let Err(error) = result {
            log::error!("Replying to request {}: {}", self.unique, error);
        }

        Done::new()
    }
}

impl<'o, O: Operation<'o>> From<(Reply<'o, O>, Errno)> for Done<'o> {
    fn from((reply, errno): (Reply<'o, O>, Errno)) -> Done<'o> {
        reply.fail(errno)
    }
}

impl<'o> FromResidual<Done<'o>> for Done<'o> {
    fn from_residual(residual: Done<'o>) -> Self {
        residual
    }
}

impl<'o, T: Into<Done<'o>>> FromResidual<Result<Infallible, T>> for Done<'o> {
    fn from_residual(residual: Result<Infallible, T>) -> Self {
        match residual {
            Ok(_) => unreachable!(),
            Err(t) => t.into(),
        }
    }
}

impl<'o, O: Operation<'o>> FromResidual<Interruptible<'o, O, Infallible>> for Done<'o> {
    fn from_residual(residual: Interruptible<'o, O, Infallible>) -> Self {
        match residual {
            Interruptible::Completed(_, _) => unreachable!(),
            Interruptible::Interrupted(done) => done,
        }
    }
}

impl Try for Done<'_> {
    type Output = Self;
    type Residual = Self;

    fn from_output(output: Self::Output) -> Self {
        output
    }

    fn branch(self) -> ControlFlow<Self::Residual, Self::Output> {
        ControlFlow::Break(self)
    }
}

impl<'o, O: Operation<'o>, T> FromResidual<Interruptible<'o, O, Infallible>>
    for Interruptible<'o, O, T>
{
    fn from_residual(residual: Interruptible<'o, O, Infallible>) -> Self {
        use Interruptible::*;

        match residual {
            Completed(_, _) => unreachable!(),
            Interrupted(done) => Interrupted(done),
        }
    }
}

impl<'o, O: Operation<'o>, T> Try for Interruptible<'o, O, T> {
    type Output = (Reply<'o, O>, T);
    type Residual = Interruptible<'o, O, Infallible>;

    fn from_output((reply, t): Self::Output) -> Self {
        Self::Completed(reply, t)
    }

    fn branch(self) -> ControlFlow<Self::Residual, Self::Output> {
        use Interruptible::*;

        match self {
            Completed(reply, t) => ControlFlow::Continue((reply, t)),
            Interrupted(done) => ControlFlow::Break(Interrupted(done)),
        }
    }
}

impl Attrs {
    #[must_use]
    pub fn size(self, size: u64) -> Self {
        Attrs(proto::Attrs { size, ..self.0 })
    }

    #[must_use]
    pub fn owner(self, uid: Uid, gid: Gid) -> Self {
        Attrs(proto::Attrs {
            uid: uid.as_raw(),
            gid: gid.as_raw(),
            ..self.0
        })
    }

    #[must_use]
    pub fn mode(self, mode: Mode) -> Self {
        Attrs(proto::Attrs {
            mode: mode.bits(),
            ..self.0
        })
    }

    #[must_use]
    pub fn blocks(self, blocks: u64) -> Self {
        Attrs(proto::Attrs { blocks, ..self.0 })
    }

    #[must_use]
    pub fn block_size(self, block_size: u32) -> Self {
        Attrs(proto::Attrs {
            blksize: block_size,
            ..self.0
        })
    }

    #[must_use]
    pub fn device(self, device: u32) -> Self {
        Attrs(proto::Attrs {
            rdev: device,
            ..self.0
        })
    }

    #[must_use]
    pub fn times(self, access: Timestamp, modify: Timestamp, change: Timestamp) -> Self {
        Attrs(proto::Attrs {
            atime: access.seconds as _,
            mtime: modify.seconds as _,
            ctime: change.seconds as _,
            atimensec: access.nanoseconds,
            mtimensec: modify.nanoseconds,
            ctimensec: change.nanoseconds,
            ..self.0
        })
    }

    #[must_use]
    pub fn links(self, links: u32) -> Self {
        Attrs(proto::Attrs {
            nlink: links,
            ..self.0
        })
    }

    pub(crate) fn finish(self, inode: &impl Stat) -> proto::Attrs {
        let Ino(ino) = inode.ino();
        let inode_type = match inode.inode_type() {
            EntryType::Fifo => SFlag::S_IFIFO,
            EntryType::CharacterDevice => SFlag::S_IFCHR,
            EntryType::Directory => SFlag::S_IFDIR,
            EntryType::BlockDevice => SFlag::S_IFBLK,
            EntryType::File => SFlag::S_IFREG,
            EntryType::Symlink => SFlag::S_IFLNK,
            EntryType::Socket => SFlag::S_IFSOCK,
        };

        proto::Attrs {
            ino,
            mode: self.0.mode | inode_type.bits(),
            ..self.0
        }
    }
}

impl Default for Attrs {
    fn default() -> Self {
        Attrs(Zeroable::zeroed()).links(1)
    }
}

impl FsInfo {
    #[must_use]
    pub fn blocks(self, size: u32, total: u64, free: u64, available: u64) -> Self {
        FsInfo(proto::StatfsOut {
            bsize: size,
            blocks: total,
            bfree: free,
            bavail: available,
            ..self.0
        })
    }

    #[must_use]
    pub fn inodes(self, total: u64, free: u64) -> Self {
        FsInfo(proto::StatfsOut {
            files: total,
            ffree: free,
            ..self.0
        })
    }

    #[must_use]
    pub fn max_filename(self, max: u32) -> Self {
        FsInfo(proto::StatfsOut {
            namelen: max,
            ..self.0
        })
    }
}

impl Default for FsInfo {
    fn default() -> Self {
        FsInfo(Zeroable::zeroed())
    }
}

impl From<FsInfo> for proto::StatfsOut {
    fn from(FsInfo(statfs): FsInfo) -> proto::StatfsOut {
        statfs
    }
}
