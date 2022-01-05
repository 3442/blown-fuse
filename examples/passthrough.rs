// Mirrors the root directory.
//
// This example is "single-threaded" in the sense that no tasks are spawned to handle potentially
// long requests.

use std::{
    collections::HashMap,
    fs::Metadata,
    ops::ControlFlow,
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::{Path, PathBuf},
};

use blown_fuse::{
    io::{Attrs, Entry, EntryType, Gid, Known, Mode, OpenFlags, Stat, Uid},
    mount::mount_sync,
    ops,
    session::{Dispatch, Start},
    Done, Errno, FuseResult, Ino, Op, Timestamp, Ttl,
};

use tokio::{
    fs::{self, DirEntry, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    runtime::Runtime,
};

use clap::{App, Arg};
use nix::unistd::mkdir;

struct Passthrough {
    known: HashMap<Ino, Inode>,
    root_dev: u64,
    open_dirs: OpenMap<OpenDir>,
    open_files: OpenMap<OpenFile>,
}

struct Inode {
    path: PathBuf,
    metadata: Metadata,
    lookup_count: u64,
}

struct OpenMap<T> {
    next: u64,
    entries: HashMap<u64, T>,
}

impl<T> OpenMap<T> {
    fn get(&mut self, handle: u64) -> Result<&mut T, Errno> {
        self.entries.get_mut(&handle).ok_or(Errno::EINVAL)
    }

    fn insert(&mut self, entry: T) -> u64 {
        let handle = self.next;
        self.entries.insert(handle, entry);

        self.next += 1;
        handle
    }
}

impl<T> Default for OpenMap<T> {
    fn default() -> Self {
        OpenMap {
            next: 0,
            entries: Default::default(),
        }
    }
}

struct OpenFile {
    handle: File,
    offset: u64,
}

impl OpenFile {
    async fn seek(&mut self, offset: u64) -> std::io::Result<()> {
        if self.offset != offset {
            self.handle.seek(std::io::SeekFrom::Start(offset)).await?;
            self.offset = offset;
        }

        Ok(())
    }
}

struct OpenDir {
    // Unfortunately, there is no seekdir() equivalent on std, nix or tokio
    children: Vec<DirEntry>,
}

struct New<'a>(&'a mut HashMap<Ino, Inode>, Inode);

impl Passthrough {
    fn known(&self, ino: Ino) -> Result<&Inode, Errno> {
        self.known.get(&ino).ok_or(Errno::ENOANO)
    }

    async fn lookup<'o>(&mut self, (request, reply): Op<'o, ops::Lookup>) -> Done<'o> {
        let (reply, parent) = reply.and_then(self.known(request.ino()))?;

        let path = parent.path.join(request.name());
        let (reply, metadata) = reply.and_then(fs::symlink_metadata(&path).await)?;

        reply.known(New(&mut self.known, Inode::new(path, metadata)), Ttl::MAX)
    }

    fn forget<'o>(&mut self, (request, reply): Op<'o, ops::Forget>) -> Done<'o> {
        use std::collections::hash_map::Entry;

        for (ino, count) in request.forget_list() {
            if let Entry::Occupied(mut entry) = self.known.entry(ino) {
                let inode = entry.get_mut();
                inode.lookup_count = inode.lookup_count.saturating_sub(count);

                if inode.lookup_count == 0 {
                    entry.remove();
                }
            }
        }

        reply.ok()
    }

    fn getattr<'o>(&mut self, (request, reply): Op<'o, ops::Getattr>) -> Done<'o> {
        let (reply, inode) = reply.and_then(self.known(request.ino()))?;
        reply.stat(inode)
    }

    async fn readlink<'o>(&mut self, (request, reply): Op<'o, ops::Readlink>) -> Done<'o> {
        let (reply, inode) = reply.and_then(self.known(request.ino()))?;
        let (reply, target) = reply.and_then(fs::read_link(&inode.path).await)?;

        reply.blob(&target)
    }

    async fn mkdir<'o>(&mut self, (request, reply): Op<'o, ops::Mkdir>) -> Done<'o> {
        let (reply, inode) = reply.and_then(self.known(request.ino()))?;
        let path = inode.path.join(request.name());

        let (reply, ()) = reply.and_then(mkdir(&path, request.mode()))?;
        let (reply, metadata) = reply.and_then(fs::symlink_metadata(&path).await)?;

        reply.known(New(&mut self.known, Inode::new(path, metadata)), Ttl::MAX)
    }

    async fn open<'o>(&mut self, (request, reply): Op<'o, ops::Open>) -> Done<'o> {
        let (reply, inode) = reply.and_then(self.known(request.ino()))?;
        let options = {
            let (flags, mut options) = (request.flags(), OpenOptions::new());
            options.custom_flags(flags.bits());

            if flags.contains(OpenFlags::O_RDONLY) || flags.contains(OpenFlags::O_RDWR) {
                options.read(true);
            }

            if flags.contains(OpenFlags::O_WRONLY) || flags.contains(OpenFlags::O_RDWR) {
                options.write(true);
            }

            if flags.contains(OpenFlags::O_TRUNC) {
                options.truncate(true);
            }

            if flags.contains(OpenFlags::O_APPEND) {
                options.truncate(true);
            }

            options
        };

        let (reply, handle) = reply.and_then(options.open(&inode.path).await)?;
        let file = OpenFile { offset: 0, handle };

        reply.ok_with_handle(self.open_files.insert(file))
    }

    async fn read<'o>(&mut self, (request, reply): Op<'o, ops::Read>) -> Done<'o> {
        // The read size may be larget than the file size
        let (reply, inode) = reply.and_then(self.known(request.ino()))?;
        let file_size = inode.metadata.len();

        let (reply, file) = reply.and_then(self.open_files.get(request.handle()))?;
        let (reply, ()) = reply.and_then(file.seek(request.offset()).await)?;

        let mut buffer = Vec::new();
        buffer.resize((request.size() as usize).min(file_size as usize), 0);

        let (reply, _) = reply.and_then(file.handle.read_exact(&mut buffer).await)?;
        reply.slice(&buffer)
    }

    async fn write<'o>(&mut self, (request, reply): Op<'o, ops::Write>) -> Done<'o> {
        let (reply, file) = reply.and_then(self.open_files.get(request.handle()))?;
        let (reply, ()) = reply.and_then(file.seek(request.offset()).await)?;
        let (reply, ()) = reply.and_then(file.handle.write_all(request.data()).await)?;

        reply.all()
    }

    fn release<'o>(&mut self, (request, reply): Op<'o, ops::Release>) -> Done<'o> {
        self.open_files.entries.remove(&request.handle());
        reply.ok()
    }

    async fn opendir<'o>(&mut self, (request, reply): Op<'o, ops::Opendir>) -> Done<'o> {
        let (reply, inode) = reply.and_then(self.known(request.ino()))?;
        let (mut reply, mut stream) = reply.and_then(fs::read_dir(&inode.path).await)?;

        let mut children = Vec::new();
        while let Some(entry) = stream.next_entry().await.transpose() {
            let (next_reply, entry) = reply.and_then(entry)?;
            reply = next_reply;

            children.push(entry);
        }

        reply.ok_with_handle(self.open_dirs.insert(OpenDir { children }))
    }

    async fn readdir<'o>(&mut self, (request, reply): Op<'o, ops::Readdir>) -> Done<'o> {
        let (reply, parent) = reply.and_then(self.known(request.ino()))?;
        let parent_path = parent.path.clone();

        let (reply, dir) = reply.and_then(self.open_dirs.get(request.handle()))?;
        let mut reply = reply.buffered(Vec::new()); //TODO: with_capacity()

        for (offset, entry) in dir
            .children
            .iter()
            .enumerate()
            .skip(request.offset() as usize)
        {
            let name = entry.file_name();
            let path = parent_path.join(&name);

            let (next_reply, metadata) = reply.and_then(entry.metadata().await)?;
            if metadata.dev() != self.root_dev {
                reply = next_reply;
                continue;
            }

            let entry = Entry {
                offset: offset as u64 + 1,
                name: &name,
                ttl: Ttl::MAX,
                inode: New(&mut self.known, Inode::new(path, metadata)),
            };

            let (next_reply, ()) = next_reply.entry(entry)?;
            reply = next_reply;
        }

        reply.end()
    }

    fn releasedir<'o>(&mut self, (request, reply): Op<'o, ops::Releasedir>) -> Done<'o> {
        self.open_dirs.entries.remove(&request.handle());
        reply.ok()
    }
}

impl Inode {
    fn new(path: PathBuf, metadata: Metadata) -> Self {
        Inode {
            path,
            metadata,
            lookup_count: 1,
        }
    }
}

impl Stat for Inode {
    fn ino(&self) -> Ino {
        Ino(self.metadata.ino())
    }

    fn inode_type(&self) -> EntryType {
        let file_type = self.metadata.file_type();

        if file_type.is_dir() {
            EntryType::Directory
        } else if file_type.is_symlink() {
            EntryType::Symlink
        } else if file_type.is_block_device() {
            EntryType::BlockDevice
        } else if file_type.is_char_device() {
            EntryType::CharacterDevice
        } else if file_type.is_fifo() {
            EntryType::Fifo
        } else if file_type.is_socket() {
            EntryType::Socket
        } else {
            assert!(file_type.is_file());
            EntryType::File
        }
    }

    fn attrs(&self) -> (Attrs, Ttl) {
        let meta = &self.metadata;

        let attrs = Attrs::default()
            .size(meta.len())
            .owner(Uid::from_raw(meta.uid()), Gid::from_raw(meta.gid()))
            .mode(Mode::from_bits_truncate(meta.mode()))
            .blocks(meta.blocks())
            .block_size(meta.blksize() as u32)
            .times(
                Timestamp::new(meta.atime(), meta.atime_nsec() as u32),
                Timestamp::new(meta.mtime(), meta.mtime_nsec() as u32),
                Timestamp::new(meta.ctime(), meta.ctime_nsec() as u32),
            )
            .links(meta.nlink() as u32)
            .device(meta.rdev() as u32);

        (attrs, Ttl::MAX)
    }
}

impl Known for New<'_> {
    type Inode = Inode;

    fn inode(&self) -> &Self::Inode {
        &self.1
    }

    fn unveil(self) {
        let New(known, inode) = self;

        known
            .entry(inode.ino())
            .and_modify(|inode| inode.lookup_count += 1)
            .or_insert(inode);
    }
}

async fn main_loop(session: Start, mut fs: Passthrough) -> FuseResult<()> {
    let session = session.start(|(_request, reply)| reply.ok()).await?;

    let mut endpoint = session.endpoint();

    loop {
        let result = endpoint.receive(|dispatch| async {
            use Dispatch::*;

            match dispatch {
                Lookup(lookup) => fs.lookup(lookup.op()?).await,
                Forget(forget) => fs.forget(forget.op()?),
                Getattr(getattr) => fs.getattr(getattr.op()?),
                Readlink(readlink) => fs.readlink(readlink.op()?).await,
                Mkdir(mkdir) => fs.mkdir(mkdir.op()?).await,
                Open(open) => fs.open(open.op()?).await,
                Read(read) => fs.read(read.op()?).await,
                Write(write) => fs.write(write.op()?).await,
                Release(release) => fs.release(release.op()?),
                Opendir(opendir) => fs.opendir(opendir.op()?).await,
                Readdir(readdir) => fs.readdir(readdir.op()?).await,
                Releasedir(releasedir) => fs.releasedir(releasedir.op()?),

                dispatch => {
                    let (_, reply) = dispatch.op();
                    reply.not_implemented()
                }
            }
        });

        match result.await? {
            ControlFlow::Break(()) => break Ok(()),
            ControlFlow::Continue(()) => continue,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new("ext2")
        .about("passthrough FUSE driver")
        .arg(Arg::from_usage("<mountpoint> 'Filesystem mountpoint'"))
        .get_matches();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let mountpoint = Path::new(matches.value_of("mountpoint").unwrap());
    let session = mount_sync(mountpoint, &Default::default())?;

    let fs = {
        let mut known = HashMap::new();

        let metadata = std::fs::metadata("/").unwrap();
        let root_dev = metadata.dev();

        known.insert(Ino::ROOT, Inode::new("/".into(), metadata));

        Passthrough {
            known,
            root_dev,
            open_dirs: Default::default(),
            open_files: Default::default(),
        }
    };

    let result = Runtime::new()?.block_on(async move {
        tokio::select! {
            result = main_loop(session, fs) => result,
            _ = tokio::signal::ctrl_c() => Ok(()),
        }
    });

    Ok(result?)
}
