use bytemuck::Zeroable;
use nix::{errno::Errno, sys::stat::SFlag};

use std::{
    borrow::Cow,
    convert::Infallible,
    ffi::OsStr,
    future::Future,
    ops::{ControlFlow, FromResidual, Try},
};

use crate::{proto, Ino, TimeToLive, Timestamp};

use super::{
    fs::{Fuse, Inode},
    session, Done, Operation, Reply, Request,
};

#[doc(no_inline)]
pub use nix::{
    dir::Type as EntryType,
    sys::stat::Mode,
    unistd::{AccessFlags, Gid, Pid, Uid},
};

pub enum Interruptible<'o, Fs: Fuse, O: Operation<'o, Fs>, T> {
    Completed(Reply<'o, Fs, O>, T),
    Interrupted(Done<'o>),
}

#[derive(Clone)]
pub struct Attrs(proto::Attrs);

pub struct Entry<'a, Ref> {
    pub offset: u64,
    pub name: Cow<'a, OsStr>,
    pub inode: Ref,
    pub ttl: TimeToLive,
}

pub struct FsInfo(proto::StatfsOut);

impl<'o, Fs: Fuse, O: Operation<'o, Fs>> Request<'o, Fs, O> {
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

impl<'o, Fs: Fuse, O: Operation<'o, Fs>> Reply<'o, Fs, O> {
    pub async fn interruptible<F, T>(self, f: F) -> Interruptible<'o, Fs, O, T>
    where
        F: Future<Output = T>,
    {
        tokio::pin!(f);
        let mut rx = session::interrupt_rx(self.session);

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

    pub fn fallible<T>(self, result: Result<T, Errno>) -> Result<(Self, T), Done<'o>> {
        match result {
            Ok(t) => Ok((self, t)),
            Err(errno) => Err(self.fail(errno)),
        }
    }

    pub fn fail(mut self, errno: Errno) -> Done<'o> {
        let errno = errno as i32;
        O::consume_errno(errno, &mut self.tail);

        Done::from_result(session::fail(self.session, self.unique, errno))
    }

    pub fn not_implemented(self) -> Done<'o> {
        self.fail(Errno::ENOSYS)
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
}

impl<'o, Fs, O> From<(Reply<'o, Fs, O>, Errno)> for Done<'o>
where
    Fs: Fuse,
    O: Operation<'o, Fs>,
{
    fn from((reply, errno): (Reply<'o, Fs, O>, Errno)) -> Done<'o> {
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

impl<'o, Fs, O> FromResidual<Interruptible<'o, Fs, O, Infallible>> for Done<'o>
where
    Fs: Fuse,
    O: Operation<'o, Fs>,
{
    fn from_residual(residual: Interruptible<'o, Fs, O, Infallible>) -> Self {
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

impl<'o, Fs, O, T> FromResidual<Interruptible<'o, Fs, O, Infallible>>
    for Interruptible<'o, Fs, O, T>
where
    Fs: Fuse,
    O: Operation<'o, Fs>,
{
    fn from_residual(residual: Interruptible<'o, Fs, O, Infallible>) -> Self {
        use Interruptible::*;

        match residual {
            Completed(_, _) => unreachable!(),
            Interrupted(done) => Interrupted(done),
        }
    }
}

impl<'o, Fs, O, T> Try for Interruptible<'o, Fs, O, T>
where
    Fs: Fuse,
    O: Operation<'o, Fs>,
{
    type Output = (Reply<'o, Fs, O>, T);
    type Residual = Interruptible<'o, Fs, O, Infallible>;

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
    pub fn size(self, size: u64) -> Self {
        Attrs(proto::Attrs { size, ..self.0 })
    }

    pub fn owner(self, uid: Uid, gid: Gid) -> Self {
        Attrs(proto::Attrs {
            uid: uid.as_raw(),
            gid: gid.as_raw(),
            ..self.0
        })
    }

    pub fn mode(self, mode: Mode) -> Self {
        Attrs(proto::Attrs {
            mode: mode.bits(),
            ..self.0
        })
    }

    pub fn blocks(self, blocks: u64, block_size: u32) -> Self {
        Attrs(proto::Attrs {
            blocks,
            blksize: block_size,
            ..self.0
        })
    }

    pub fn times(self, access: Timestamp, modify: Timestamp, change: Timestamp) -> Self {
        Attrs(proto::Attrs {
            atime: access.seconds,
            mtime: modify.seconds,
            ctime: change.seconds,
            atimensec: access.nanoseconds,
            mtimensec: modify.nanoseconds,
            ctimensec: change.nanoseconds,
            ..self.0
        })
    }

    pub fn links(self, links: u32) -> Self {
        Attrs(proto::Attrs {
            nlink: links,
            ..self.0
        })
    }

    pub(crate) fn finish<Fs: Fuse>(self, inode: &Fs::Farc) -> proto::Attrs {
        let Ino(ino) = <Fs as Fuse>::Inode::ino(inode);
        let inode_type = match <Fs as Fuse>::Inode::inode_type(inode) {
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
    pub fn blocks(self, size: u32, total: u64, free: u64, available: u64) -> Self {
        FsInfo(proto::StatfsOut {
            bsize: size,
            blocks: total,
            bfree: free,
            bavail: available,
            ..self.0
        })
    }

    pub fn inodes(self, total: u64, free: u64) -> Self {
        FsInfo(proto::StatfsOut {
            files: total,
            ffree: free,
            ..self.0
        })
    }

    pub fn filenames(self, max: u32) -> Self {
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
