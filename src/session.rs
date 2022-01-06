use std::{
    future::Future,
    io,
    marker::PhantomData,
    ops::ControlFlow,
    os::unix::io::{IntoRawFd, RawFd},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use nix::{
    fcntl::{fcntl, FcntlArg, OFlag},
    sys::uio::{writev, IoVec},
    unistd::read,
};

use tokio::{
    io::unix::AsyncFd,
    sync::{broadcast, OwnedSemaphorePermit, Semaphore},
};

use bytemuck::bytes_of;
use smallvec::SmallVec;

use crate::{
    error::MountError,
    mount::unmount_sync,
    proto::{self, InHeader, Structured},
    util::{page_size, DumbFd, OutputChain},
    Errno, FuseError, FuseResult,
};

use super::{
    ops::{self, FromRequest},
    Done, Op, Operation, Reply, Request,
};

pub struct Start {
    session_fd: DumbFd,
    mountpoint: PathBuf,
}

pub struct Session {
    session_fd: AsyncFd<RawFd>,
    interrupt_tx: broadcast::Sender<u64>,
    buffers: Mutex<Vec<Buffer>>,
    buffer_semaphore: Arc<Semaphore>,
    buffer_pages: usize,
    mountpoint: Mutex<Option<PathBuf>>,
}

pub struct Endpoint<'a> {
    session: &'a Arc<Session>,
    local_buffer: Buffer,
}

pub enum Dispatch<'o> {
    Lookup(Incoming<'o, ops::Lookup>),
    Forget(Incoming<'o, ops::Forget>),
    Getattr(Incoming<'o, ops::Getattr>),
    Readlink(Incoming<'o, ops::Readlink>),
    Symlink(Incoming<'o, ops::Symlink>),
    Mkdir(Incoming<'o, ops::Mkdir>),
    Unlink(Incoming<'o, ops::Unlink>),
    Rmdir(Incoming<'o, ops::Rmdir>),
    Link(Incoming<'o, ops::Link>),
    Open(Incoming<'o, ops::Open>),
    Read(Incoming<'o, ops::Read>),
    Write(Incoming<'o, ops::Write>),
    Statfs(Incoming<'o, ops::Statfs>),
    Release(Incoming<'o, ops::Release>),
    Fsync(Incoming<'o, ops::Fsync>),
    Setxattr(Incoming<'o, ops::Setxattr>),
    Getxattr(Incoming<'o, ops::Getxattr>),
    Listxattr(Incoming<'o, ops::Listxattr>),
    Removexattr(Incoming<'o, ops::Removexattr>),
    Flush(Incoming<'o, ops::Flush>),
    Opendir(Incoming<'o, ops::Opendir>),
    Readdir(Incoming<'o, ops::Readdir>),
    Releasedir(Incoming<'o, ops::Releasedir>),
    Fsyncdir(Incoming<'o, ops::Fsyncdir>),
    Access(Incoming<'o, ops::Access>),
    Create(Incoming<'o, ops::Create>),
}

pub struct Incoming<'o, O: Operation<'o>> {
    common: IncomingCommon<'o>,
    _phantom: PhantomData<O>,
}

pub struct Owned<O> {
    session: Arc<Session>,
    buffer: Buffer,
    header: InHeader,
    _permit: OwnedSemaphorePermit,
    _phantom: PhantomData<O>,
}

impl Session {
    // Does not seem like 'a can be elided here
    #[allow(clippy::needless_lifetimes)]
    pub fn endpoint<'a>(self: &'a Arc<Self>) -> Endpoint<'a> {
        Endpoint {
            session: self,
            local_buffer: Buffer::new(self.buffer_pages),
        }
    }

    pub fn unmount_sync(&self) -> Result<(), MountError> {
        let mountpoint = self.mountpoint.lock().unwrap().take();
        if let Some(mountpoint) = &mountpoint {
            unmount_sync(mountpoint)?;
        }

        Ok(())
    }

    pub(crate) fn ok(&self, unique: u64, output: OutputChain<'_>) -> FuseResult<()> {
        self.send(unique, 0, output)
    }

    pub(crate) fn fail(&self, unique: u64, mut errno: i32) -> FuseResult<()> {
        if errno <= 0 {
            log::warn!(
                "Attempted to fail req#{} with errno {} <= 0, coercing to ENOMSG",
                unique,
                errno
            );

            errno = Errno::ENOMSG as i32;
        }

        self.send(unique, -errno, OutputChain::empty())
    }

    pub(crate) fn interrupt_rx(&self) -> broadcast::Receiver<u64> {
        self.interrupt_tx.subscribe()
    }

    async fn handshake<F>(&mut self, buffer: &mut Buffer, init: F) -> FuseResult<Handshake<F>>
    where
        F: FnOnce(Op<'_, ops::Init>) -> Done<'_>,
    {
        self.session_fd.readable().await?.retain_ready();
        let bytes = read(*self.session_fd.get_ref(), &mut buffer.0).map_err(io::Error::from)?;

        let (header, opcode) = InHeader::from_bytes(&buffer.0[..bytes])?;
        let body = match opcode {
            proto::Opcode::Init => {
                <&proto::InitIn>::toplevel_from(&buffer.0[HEADER_END..bytes], &header)?
            }

            _ => {
                log::error!("First message from kernel is not Init, but {:?}", opcode);
                return Err(FuseError::ProtocolInit);
            }
        };

        use std::cmp::Ordering;
        let supported = match body.major.cmp(&proto::MAJOR_VERSION) {
            Ordering::Less => false,
            Ordering::Equal => body.minor >= proto::REQUIRED_MINOR_VERSION,
            Ordering::Greater => {
                let tail = [bytes_of(&proto::MAJOR_VERSION)];
                self.ok(header.unique, OutputChain::tail(&tail))?;

                return Ok(Handshake::Restart(init));
            }
        };

        //TODO: fake some decency by supporting a few older minor versions
        if !supported {
            log::error!(
                "Unsupported protocol {}.{}; this build requires \
                 {major}.{}..={major}.{} (or a greater version \
                 through compatibility)",
                body.major,
                body.minor,
                proto::REQUIRED_MINOR_VERSION,
                proto::TARGET_MINOR_VERSION,
                major = proto::MAJOR_VERSION
            );

            self.fail(header.unique, Errno::EPROTONOSUPPORT as i32)?;
            return Err(FuseError::ProtocolInit);
        }

        let request = Request { header, body };
        let reply = Reply {
            session: self,
            unique: header.unique,
            state: ops::InitState {
                kernel_flags: proto::InitFlags::from_bits_truncate(body.flags),
                buffer_pages: self.buffer_pages,
            },
        };

        init((request, reply)).consume();
        Ok(Handshake::Done)
    }

    fn send(&self, unique: u64, error: i32, output: OutputChain<'_>) -> FuseResult<()> {
        let after_header: usize = output
            .iter()
            .flat_map(<[_]>::iter)
            .copied()
            .map(<[_]>::len)
            .sum();

        let length = (std::mem::size_of::<proto::OutHeader>() + after_header) as _;
        let header = proto::OutHeader {
            len: length,
            error,
            unique,
        };

        let header = [bytes_of(&header)];
        let output = output.preceded(&header);
        let buffers: SmallVec<[_; 8]> = output
            .iter()
            .flat_map(<[_]>::iter)
            .copied()
            .filter(|slice| !slice.is_empty())
            .map(IoVec::from_slice)
            .collect();

        let written = writev(*self.session_fd.get_ref(), &buffers).map_err(io::Error::from)?;
        if written == length as usize {
            Ok(())
        } else {
            Err(FuseError::ShortWrite)
        }
    }
}

impl Drop for Start {
    fn drop(&mut self) {
        if !self.mountpoint.as_os_str().is_empty() {
            let _ = unmount_sync(&self.mountpoint);
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(mountpoint) = self.mountpoint.get_mut().unwrap().take() {
            let _ = unmount_sync(&mountpoint);
        }

        drop(DumbFd(*self.session_fd.get_ref())); // Close
    }
}

impl<'o> Dispatch<'o> {
    pub fn op(self) -> Op<'o> {
        use Dispatch::*;

        let common = match self {
            Lookup(incoming) => incoming.common,
            Forget(incoming) => incoming.common,
            Getattr(incoming) => incoming.common,
            Readlink(incoming) => incoming.common,
            Symlink(incoming) => incoming.common,
            Mkdir(incoming) => incoming.common,
            Unlink(incoming) => incoming.common,
            Rmdir(incoming) => incoming.common,
            Link(incoming) => incoming.common,
            Open(incoming) => incoming.common,
            Read(incoming) => incoming.common,
            Write(incoming) => incoming.common,
            Statfs(incoming) => incoming.common,
            Release(incoming) => incoming.common,
            Fsync(incoming) => incoming.common,
            Setxattr(incoming) => incoming.common,
            Getxattr(incoming) => incoming.common,
            Listxattr(incoming) => incoming.common,
            Removexattr(incoming) => incoming.common,
            Flush(incoming) => incoming.common,
            Opendir(incoming) => incoming.common,
            Readdir(incoming) => incoming.common,
            Releasedir(incoming) => incoming.common,
            Fsyncdir(incoming) => incoming.common,
            Access(incoming) => incoming.common,
            Create(incoming) => incoming.common,
        };

        common.into_generic_op()
    }
}

impl Endpoint<'_> {
    pub async fn receive<'o, F, Fut>(&'o mut self, dispatcher: F) -> FuseResult<ControlFlow<()>>
    where
        F: FnOnce(Dispatch<'o>) -> Fut,
        Fut: Future<Output = Done<'o>>,
    {
        let buffer = &mut self.local_buffer.0;
        let bytes = loop {
            let session_fd = &self.session.session_fd;

            let mut readable = tokio::select! {
                readable = session_fd.readable() => readable?,

                _ = session_fd.writable() => {
                    self.session.mountpoint.lock().unwrap().take();
                    return Ok(ControlFlow::Break(()));
                }
            };

            let mut read = |fd: &AsyncFd<RawFd>| read(*fd.get_ref(), buffer);
            let result = match readable.try_io(|fd| read(fd).map_err(io::Error::from)) {
                Ok(result) => result,
                Err(_) => continue,
            };

            match result {
                // Interrupted
                //TODO: libfuse docs say that this has some side effects
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,

                result => break result,
            }
        };

        let (header, opcode) = InHeader::from_bytes(&buffer[..bytes?])?;
        let common = IncomingCommon {
            session: self.session,
            buffer: &mut self.local_buffer,
            header,
        };

        let dispatch = {
            use proto::Opcode::*;

            macro_rules! dispatch {
                ($op:ident) => {
                    Dispatch::$op(Incoming {
                        common,
                        _phantom: PhantomData,
                    })
                };
            }

            match opcode {
                Destroy => return Ok(ControlFlow::Break(())),

                Lookup => dispatch!(Lookup),
                Forget => dispatch!(Forget),
                Getattr => dispatch!(Getattr),
                Readlink => dispatch!(Readlink),
                Symlink => dispatch!(Symlink),
                Mkdir => dispatch!(Mkdir),
                Unlink => dispatch!(Unlink),
                Rmdir => dispatch!(Rmdir),
                Link => dispatch!(Link),
                Open => dispatch!(Open),
                Read => dispatch!(Read),
                Write => dispatch!(Write),
                Statfs => dispatch!(Statfs),
                Release => dispatch!(Release),
                Fsync => dispatch!(Fsync),
                Setxattr => dispatch!(Setxattr),
                Getxattr => dispatch!(Getxattr),
                Listxattr => dispatch!(Listxattr),
                Removexattr => dispatch!(Removexattr),
                Flush => dispatch!(Flush),
                Opendir => dispatch!(Opendir),
                Readdir => dispatch!(Readdir),
                Releasedir => dispatch!(Releasedir),
                Fsyncdir => dispatch!(Fsyncdir),
                Access => dispatch!(Access),
                Create => dispatch!(Create),
                BatchForget => dispatch!(Forget),
                ReaddirPlus => dispatch!(Readdir),

                _ => {
                    log::warn!("Not implemented: {}", common.header);

                    let (_request, reply) = common.into_generic_op();
                    reply.not_implemented().consume();

                    return Ok(ControlFlow::Continue(()));
                }
            }
        };

        dispatcher(dispatch).await.consume();
        Ok(ControlFlow::Continue(()))
    }
}

impl Start {
    pub async fn start<F>(mut self, mut init: F) -> FuseResult<Arc<Session>>
    where
        F: FnOnce(Op<'_, ops::Init>) -> Done<'_>,
    {
        let mountpoint = std::mem::take(&mut self.mountpoint);
        let session_fd = self.session_fd.take().into_raw_fd();

        let flags = OFlag::O_NONBLOCK | OFlag::O_LARGEFILE;
        fcntl(session_fd, FcntlArg::F_SETFL(flags)).unwrap();

        let (interrupt_tx, _) = broadcast::channel(INTERRUPT_BROADCAST_CAPACITY);

        let buffer_pages = proto::MIN_READ_SIZE / page_size(); //TODO
        let buffer_count = SHARED_BUFFERS; //TODO
        let buffers = std::iter::repeat_with(|| Buffer::new(buffer_pages))
            .take(buffer_count)
            .collect();

        let mut session = Session {
            session_fd: AsyncFd::with_interest(session_fd, tokio::io::Interest::READABLE)?,
            interrupt_tx,
            buffers: Mutex::new(buffers),
            buffer_semaphore: Arc::new(Semaphore::new(buffer_count)),
            buffer_pages,
            mountpoint: Mutex::new(Some(mountpoint)),
        };

        let mut init_buffer = session.buffers.get_mut().unwrap().pop().unwrap();

        loop {
            init = match session.handshake(&mut init_buffer, init).await? {
                Handshake::Restart(init) => init,
                Handshake::Done => {
                    session.buffers.get_mut().unwrap().push(init_buffer);
                    break Ok(Arc::new(session));
                }
            };
        }
    }

    pub fn unmount_sync(mut self) -> Result<(), MountError> {
        // This prevents Start::drop() from unmounting a second time
        let mountpoint = std::mem::take(&mut self.mountpoint);
        unmount_sync(&mountpoint)
    }

    pub(crate) fn new(session_fd: DumbFd, mountpoint: PathBuf) -> Self {
        Start {
            session_fd,
            mountpoint,
        }
    }
}

impl<'o, O: Operation<'o>> Incoming<'o, O>
where
    O::ReplyState: FromRequest<'o, O>,
{
    pub fn op(self) -> Result<Op<'o, O>, Done<'o>> {
        try_op(
            self.common.session,
            &self.common.buffer.0,
            self.common.header,
        )
    }

    pub async fn owned(self) -> (Done<'o>, Owned<O>) {
        let session = self.common.session;

        let (buffer, permit) = {
            let semaphore = Arc::clone(&session.buffer_semaphore);
            let permit = semaphore
                .acquire_owned()
                .await
                .expect("Buffer semaphore error");

            let mut buffers = session.buffers.lock().unwrap();
            let buffer = buffers.pop().expect("Buffer semaphore out of sync");
            let buffer = std::mem::replace(self.common.buffer, buffer);

            (buffer, permit)
        };

        let owned = Owned {
            session: Arc::clone(session),
            buffer,
            header: self.common.header,
            _permit: permit,
            _phantom: PhantomData,
        };

        (Done::new(), owned)
    }
}

impl<O: for<'o> Operation<'o>> Owned<O>
where
    for<'o> <O as Operation<'o>>::ReplyState: FromRequest<'o, O>,
{
    pub async fn op<'o, F, Fut>(&'o self, handler: F)
    where
        F: FnOnce(Op<'o, O>) -> Fut,
        Fut: Future<Output = Done<'o>>,
    {
        match try_op(&self.session, &self.buffer.0, self.header) {
            Ok(op) => handler(op).await.consume(),
            Err(done) => done.consume(),
        }
    }
}

impl<O> Drop for Owned<O> {
    fn drop(&mut self) {
        if let Ok(mut buffers) = self.session.buffers.lock() {
            let empty = Buffer(Vec::new().into_boxed_slice());
            buffers.push(std::mem::replace(&mut self.buffer, empty));
        }
    }
}

const INTERRUPT_BROADCAST_CAPACITY: usize = 32;
const SHARED_BUFFERS: usize = 32;
const HEADER_END: usize = std::mem::size_of::<InHeader>();

struct IncomingCommon<'o> {
    session: &'o Arc<Session>,
    buffer: &'o mut Buffer,
    header: InHeader,
}

enum Handshake<F> {
    Done,
    Restart(F),
}

struct Buffer(Box<[u8]>);

impl<'o> IncomingCommon<'o> {
    fn into_generic_op(self) -> Op<'o> {
        let request = Request {
            header: self.header,
            body: (),
        };

        let reply = Reply {
            session: self.session,
            unique: self.header.unique,
            state: (),
        };

        (request, reply)
    }
}

impl Buffer {
    fn new(pages: usize) -> Self {
        Buffer(vec![0; pages * page_size()].into_boxed_slice())
    }
}

fn try_op<'o, O: Operation<'o>>(
    session: &'o Session,
    bytes: &'o [u8],
    header: InHeader,
) -> Result<Op<'o, O>, Done<'o>>
where
    O::ReplyState: FromRequest<'o, O>,
{
    let body = match Structured::toplevel_from(&bytes[HEADER_END..header.len as usize], &header) {
        Ok(body) => body,
        Err(error) => {
            log::error!("Parsing request {}: {:?}", header, error);
            let reply = Reply::<ops::Any> {
                session,
                unique: header.unique,
                state: (),
            };

            return Err(reply.io_error());
        }
    };

    let request = Request { header, body };
    let reply = Reply {
        session,
        unique: header.unique,
        state: FromRequest::from_request(&request),
    };

    Ok((request, reply))
}
