use std::{
    collections::{hash_map, HashMap},
    convert::TryInto,
    io,
    os::unix::io::{IntoRawFd, RawFd},
    sync::{Arc, Mutex},
};

use nix::{
    fcntl::{fcntl, FcntlArg, OFlag},
    sys::uio::{readv, writev, IoVec},
    unistd::{sysconf, SysconfVar},
};

use tokio::{
    io::unix::AsyncFd,
    sync::{broadcast, Notify, OwnedSemaphorePermit, Semaphore},
};

use bytemuck::{bytes_of, try_from_bytes};
use smallvec::SmallVec;

use crate::{
    proto::{self, InHeader},
    util::{display_or, OutputChain},
    Errno, FuseError, FuseResult, Ino,
};

use super::{
    fs::{Fuse, Inode},
    Reply, Request, Session, Start,
};

pub fn ok<Fs: Fuse>(session: &Session<Fs>, unique: u64, output: OutputChain<'_>) -> FuseResult<()> {
    session.send(unique, 0, output)
}

pub fn fail<Fs: Fuse>(session: &Session<Fs>, unique: u64, mut errno: i32) -> FuseResult<()> {
    if errno <= 0 {
        log::warn!(
            "Attempted to fail req#{} with errno {} <= 0, coercing to ENOMSG",
            unique,
            errno
        );

        errno = Errno::ENOMSG as i32;
    }

    session.send(unique, -errno, OutputChain::empty())
}

pub fn unveil<Fs: Fuse>(session: &Session<Fs>, inode: &Fs::Farc) {
    let ino = <Fs as Fuse>::Inode::ino(inode);
    let mut known = session.known.lock().unwrap();

    use hash_map::Entry::*;
    match known.entry(ino) {
        Occupied(entry) => {
            let (_, count) = entry.into_mut();
            *count += 1;
        }

        Vacant(entry) => {
            entry.insert((Fs::Farc::clone(inode), 1));
        }
    }
}

pub fn interrupt_rx<Fs: Fuse>(session: &Session<Fs>) -> broadcast::Receiver<u64> {
    session.interrupt_tx.subscribe()
}

impl<Fs: Fuse> Session<Fs> {
    pub fn fs(&self) -> &Fs {
        &self.fs
    }

    pub async fn main_loop(self: Arc<Self>) -> FuseResult<()> {
        let this = Arc::clone(&self);
        let main_loop = async move {
            loop {
                let incoming = this.receive().await;
                let this = Arc::clone(&this);

                tokio::spawn(async move {
                    let (result, header): (FuseResult<()>, Option<InHeader>) = match incoming {
                        Ok(mut incoming) => match this.dispatch(&mut incoming).await {
                            Ok(()) => (Ok(()), None),

                            Err(error) => {
                                let data = incoming.buffer.data();
                                let data = &data[..std::mem::size_of::<InHeader>().max(data.len())];
                                (Err(error), try_from_bytes(data).ok().copied())
                            }
                        },

                        Err(error) => (Err(error.into()), None),
                    };

                    let header = display_or(header, "(bad)");
                    if let Err(error) = result {
                        log::error!("Handling request {}: {}", header, error);
                    }
                });
            }
        };

        tokio::select! {
            () = main_loop => unreachable!(),
            () = self.destroy.notified() => Ok(()),
        }
    }

    async fn do_handshake(
        &mut self,
        pages_per_buffer: usize,
        bytes_per_buffer: usize,
    ) -> FuseResult<Handshake> {
        use FuseError::*;

        let buffer = {
            self.session_fd.readable().await?.retain_ready();
            let large_buffer = self.large_buffers.get_mut().unwrap().first_mut().unwrap();

            let mut data = InputBufferStorage::Sbo(SboStorage([0; SBO_SIZE]));
            let sbo = match &mut data {
                InputBufferStorage::Sbo(SboStorage(sbo)) => sbo,
                _ => unreachable!(),
            };

            let mut io_vecs = [
                IoVec::from_mut_slice(sbo),
                IoVec::from_mut_slice(large_buffer),
            ];

            let bytes = readv(*self.session_fd.get_ref(), &mut io_vecs).map_err(io::Error::from)?;
            InputBuffer { bytes, data }
        };

        let request: proto::Request<'_> = buffer.data().try_into()?;

        let unique = request.header().unique;
        let init = match request.body() {
            proto::RequestBody::Init(body) => body,
            _ => return Err(ProtocolInit),
        };

        use std::cmp::Ordering;
        let supported = match init.major.cmp(&proto::MAJOR_VERSION) {
            Ordering::Less => false,

            Ordering::Equal => {
                self.proto_minor = init.minor;
                self.proto_minor >= proto::REQUIRED_MINOR_VERSION
            }

            Ordering::Greater => {
                let tail = [bytes_of(&proto::MAJOR_VERSION)];
                ok(self, unique, OutputChain::tail(&tail))?;

                return Ok(Handshake::Restart);
            }
        };

        //TODO: fake some decency by supporting a few older minor versions
        if !supported {
            log::error!(
                "Unsupported protocol {}.{}; this build requires \
                 {major}.{}..={major}.{} (or a greater version \
                 through compatibility)",
                init.major,
                init.minor,
                proto::REQUIRED_MINOR_VERSION,
                proto::TARGET_MINOR_VERSION,
                major = proto::MAJOR_VERSION
            );

            fail(self, unique, Errno::EPROTONOSUPPORT as i32)?;
            return Err(ProtocolInit);
        }

        let root = {
            let mut init_result = Err(0);
            let reply = Reply {
                session: self,
                unique,
                tail: &mut init_result,
            };

            self.fs.init(reply).await.into_result()?;

            match init_result {
                Ok(root) => root,
                Err(errno) => {
                    log::error!("init() handler failed: {}", Errno::from_i32(errno));
                    return Err(FuseError::Io(std::io::Error::from_raw_os_error(errno)));
                }
            }
        };

        self.known.get_mut().unwrap().insert(Ino::ROOT, (root, 1));

        use proto::InitFlags;
        let flags = InitFlags::from_bits_truncate(init.flags);
        let supported = InitFlags::PARALLEL_DIROPS
            | InitFlags::ABORT_ERROR
            | InitFlags::MAX_PAGES
            | InitFlags::CACHE_SYMLINKS;

        let flags = flags & supported;
        let max_write = bytes_per_buffer - std::mem::size_of::<(InHeader, proto::WriteIn)>();
        let init_out = proto::InitOut {
            major: proto::MAJOR_VERSION,
            minor: proto::TARGET_MINOR_VERSION,
            max_readahead: 0, //TODO
            flags: flags.bits(),
            max_background: 0,       //TODO
            congestion_threshold: 0, //TODO
            max_write: max_write.try_into().unwrap(),
            time_gran: 1, //TODO
            max_pages: pages_per_buffer.try_into().unwrap(),
            padding: Default::default(),
            unused: Default::default(),
        };

        let tail = [bytes_of(&init_out)];
        ok(self, unique, OutputChain::tail(&tail))?;

        Ok(Handshake::Done)
    }

    async fn dispatch(self: &Arc<Self>, request: &mut Incoming) -> FuseResult<()> {
        let request: proto::Request<'_> = request.buffer.data().try_into()?;
        let header = request.header();
        let InHeader { unique, ino, .. } = *header;
        let ino = Ino(ino);

        use proto::RequestBody::*;

        macro_rules! op {
            () => {
                op!(())
            };

            ($body:expr) => {
                op!($body, ())
            };

            ($body:expr, $tail:expr) => {{
                let request = Request {
                    header,
                    body: $body,
                };
                let reply = Reply {
                    session: &self,
                    unique,
                    tail: $tail,
                };

                (request, reply, self)
            }};
        }

        // These operations don't involve locking and searching self.known
        match request.body() {
            Forget(body) => {
                self.forget(std::iter::once((ino, body.nlookup))).await;
                return Ok(());
            }

            Statfs => return self.fs.statfs(op!()).await.into_result(),

            Interrupt(body) => {
                //TODO: Don't reply with EAGAIN if the interrupt is successful
                let _ = self.interrupt_tx.send(body.unique);
                return fail(self, unique, Errno::EAGAIN as i32);
            }

            Destroy => {
                self.destroy.notify_one();
                return Ok(());
            }

            BatchForget { forgets, .. } => {
                let forgets = forgets
                    .iter()
                    .map(|target| (Ino(target.ino), target.nlookup));

                self.forget(forgets).await;
                return Ok(());
            }

            _ => (),
        }

        // Some operations are handled while self.known is locked
        let inode = {
            let known = self.known.lock().unwrap();
            let inode = match known.get(&ino) {
                Some((farc, _)) => farc,
                None => {
                    log::error!(
                        "Lookup count for ino {} reached zero while still \
                         known to the kernel, this is a bug",
                        ino
                    );

                    return fail(self, unique, Errno::ENOANO as i32);
                }
            };

            match request.body() {
                Getattr(_) => {
                    //TODO: Getattr flags
                    let (attrs, ttl) = <Fs as Fuse>::Inode::attrs(inode);
                    let attrs = attrs.finish::<Fs>(inode);
                    drop(known);

                    let out = proto::AttrOut {
                        attr_valid: ttl.seconds,
                        attr_valid_nsec: ttl.nanoseconds,
                        dummy: Default::default(),
                        attr: attrs,
                    };

                    return ok(self, unique, OutputChain::tail(&[bytes_of(&out)]));
                }

                Access(body) => {
                    return <Fs as Fuse>::Inode::access(inode, op!(*body)).into_result()
                }

                _ => inode.clone(),
            }
        };

        macro_rules! inode_op {
            ($op:ident, $($exprs:expr),+) => {
                <Fs as Fuse>::Inode::$op(inode, op!($($exprs),+)).await
            };
        }

        // These operations involve a Farc cloned from self.known
        let done = match request.body() {
            Lookup { name } => inode_op!(lookup, *name),
            Readlink => inode_op!(readlink, ()),
            Open(body) => {
                let mut flags = proto::OpenOutFlags::empty();
                if <Fs as Fuse>::Inode::direct_io(&inode) {
                    flags |= proto::OpenOutFlags::DIRECT_IO;
                }

                inode_op!(open, *body, (ino, flags))
            }
            Opendir(body) => inode_op!(opendir, *body),
            Readdir(body) => inode_op!(readdir, *body),

            _ => return fail(self, unique, Errno::ENOSYS as i32),
        };

        done.into_result()
    }

    async fn forget<I>(&self, targets: I)
    where
        I: Iterator<Item = (Ino, u64)>,
    {
        let mut known = self.known.lock().unwrap();

        for (ino, subtracted) in targets {
            use hash_map::Entry::*;

            match known.entry(ino) {
                Occupied(mut entry) => {
                    let (_, count) = entry.get_mut();

                    *count = count.saturating_sub(subtracted);
                    if *count > 0 {
                        continue;
                    }

                    entry.remove();
                }

                Vacant(_) => {
                    log::warn!("Kernel attempted to forget {:?} (bad refcount?)", ino);
                    continue;
                }
            }
        }
    }

    async fn receive(self: &Arc<Self>) -> std::io::Result<Incoming> {
        use InputBufferStorage::*;

        let permit = Arc::clone(&self.input_semaphore)
            .acquire_owned()
            .await
            .unwrap();

        let mut incoming = Incoming {
            buffer: InputBuffer {
                bytes: 0,
                data: Sbo(SboStorage([0; SBO_SIZE])),
            },
        };

        let sbo = match &mut incoming.buffer.data {
            Sbo(SboStorage(sbo)) => sbo,
            _ => unreachable!(),
        };

        loop {
            let mut readable = self.session_fd.readable().await?;

            let mut large_buffers = self.large_buffers.lock().unwrap();
            let large_buffer = large_buffers.last_mut().unwrap();

            let mut io_vecs = [
                IoVec::from_mut_slice(sbo),
                IoVec::from_mut_slice(&mut large_buffer[SBO_SIZE..]),
            ];

            let mut read = |fd: &AsyncFd<RawFd>| readv(*fd.get_ref(), &mut io_vecs);
            match readable.try_io(|fd| read(fd).map_err(io::Error::from)) {
                Ok(Ok(bytes)) => {
                    if bytes > SBO_SIZE {
                        (&mut large_buffer[..SBO_SIZE]).copy_from_slice(sbo);
                        incoming.buffer.data = Spilled(large_buffers.pop().unwrap(), permit);
                    }

                    incoming.buffer.bytes = bytes;
                    return Ok(incoming);
                }

                // Interrupted
                Ok(Err(error)) if error.kind() == std::io::ErrorKind::NotFound => continue,

                Ok(Err(error)) => return Err(error),
                Err(_) => continue,
            }
        }
    }

    fn send(&self, unique: u64, error: i32, output: OutputChain<'_>) -> FuseResult<()> {
        let after_header: usize = output
            .iter()
            .map(<[_]>::iter)
            .flatten()
            .copied()
            .map(<[_]>::len)
            .sum();

        let length = (std::mem::size_of::<proto::OutHeader>() + after_header) as _;
        let header = proto::OutHeader {
            len: length,
            error,
            unique,
        };

        //TODO: Full const generics any time now? Fs::EXPECTED_REQUEST_SEGMENTS
        let header = [bytes_of(&header)];
        let output = output.preceded(&header);
        let buffers: SmallVec<[_; 8]> = output
            .iter()
            .map(<[_]>::iter)
            .flatten()
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

impl Start {
    pub async fn start<Fs: Fuse>(self, fs: Fs) -> FuseResult<Arc<Session<Fs>>> {
        let session_fd = self.session_fd.into_raw_fd();

        let flags = OFlag::O_NONBLOCK | OFlag::O_LARGEFILE;
        fcntl(session_fd, FcntlArg::F_SETFL(flags)).unwrap();

        let page_size = sysconf(SysconfVar::PAGE_SIZE).unwrap().unwrap() as usize;
        let pages_per_buffer = fs.request_buffer_pages().get();
        let bytes_per_buffer = pages_per_buffer.checked_mul(page_size).unwrap();
        assert!(bytes_per_buffer >= proto::MIN_READ_SIZE);

        let mut large_buffers = Vec::with_capacity(fs.request_buffers().get());
        for _ in 0..large_buffers.capacity() {
            large_buffers.push(vec![0; bytes_per_buffer].into_boxed_slice());
        }

        let (interrupt_tx, _) = broadcast::channel(INTERRUPT_BROADCAST_CAPACITY);
        let mut session = Session {
            _fusermount_fd: self.fusermount_fd,
            session_fd: AsyncFd::with_interest(session_fd, tokio::io::Interest::READABLE)?,
            proto_minor: 0, // Set by Session::do_handshake()
            fs,
            input_semaphore: Arc::new(Semaphore::new(large_buffers.len())),
            large_buffers: Mutex::new(large_buffers),
            known: Mutex::new(HashMap::new()),
            destroy: Notify::new(),
            interrupt_tx,
        };

        loop {
            let state = session
                .do_handshake(pages_per_buffer, bytes_per_buffer)
                .await?;

            if let Handshake::Done = state {
                break Ok(Arc::new(session));
            }
        }
    }
}

enum Handshake {
    Done,
    Restart,
}

struct Incoming {
    buffer: InputBuffer,
}

struct InputBuffer {
    pub bytes: usize,
    pub data: InputBufferStorage,
}

enum InputBufferStorage {
    Sbo(SboStorage),
    Spilled(Box<[u8]>, OwnedSemaphorePermit),
}

#[repr(align(8))]
struct SboStorage(pub [u8; 4 * std::mem::size_of::<InHeader>()]);

const SBO_SIZE: usize = std::mem::size_of::<SboStorage>();
const INTERRUPT_BROADCAST_CAPACITY: usize = 32;

impl InputBuffer {
    fn data(&self) -> &[u8] {
        use InputBufferStorage::*;
        let storage = match &self.data {
            Sbo(sbo) => &sbo.0,
            Spilled(buffer, _) => &buffer[..],
        };

        &storage[..self.bytes]
    }
}
