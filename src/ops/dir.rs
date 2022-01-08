use std::{
    convert::Infallible,
    ffi::{CStr, OsStr},
    marker::PhantomData,
    os::unix::ffi::OsStrExt,
};

use crate::{
    io::{Entry, EntryType, Interruptible, Known, Stat},
    sealed::Sealed,
    Done, Operation, Reply, Request,
};

use super::{
    c_to_os, make_entry,
    traits::{
        ReplyBuffered, ReplyKnown, ReplyNotFound, RequestHandle, RequestName, RequestOffset,
        RequestSize,
    },
    FromRequest,
};

use crate::{proto, Errno, Ino, Ttl};
use bytemuck::{bytes_of, Zeroable};
use bytes::BufMut;
use nix::sys::stat::SFlag;

pub enum Lookup {}
pub enum Readdir {}
pub struct BufferedReaddir<B>(Infallible, PhantomData<B>);

pub trait ReplyFound<'o>: ReplyKnown<'o> {
    fn not_found_for(reply: Reply<'o, Self>, ttl: Ttl) -> Done<'o>;
}

pub trait ReplyEntries<'o>: Operation<'o> {
    fn entry(reply: Reply<'o, Self>, entry: Entry<impl Known>) -> Interruptible<'o, Self, ()>;
    fn end(reply: Reply<'o, Self>) -> Done<'o>;
}

pub struct ReaddirState<B> {
    max_read: usize,
    is_plus: bool,
    buffer: B,
}

impl Sealed for Lookup {}
impl Sealed for Readdir {}
impl<B> Sealed for BufferedReaddir<B> {}

impl<'o> Operation<'o> for Lookup {
    type RequestBody = &'o CStr; // name()
    type ReplyState = ();
}

impl<'o> Operation<'o> for Readdir {
    type RequestBody = proto::OpcodeSelect<
        &'o proto::ReaddirPlusIn,
        &'o proto::ReaddirIn,
        { proto::Opcode::ReaddirPlus as u32 },
    >;

    type ReplyState = ReaddirState<()>;
}

impl<'o, B> Operation<'o> for BufferedReaddir<B> {
    type RequestBody = (); // Never actually created
    type ReplyState = ReaddirState<B>;
}

impl<'o> RequestName<'o> for Lookup {
    fn name<'a>(request: &'a Request<'o, Self>) -> &'a OsStr {
        c_to_os(request.body)
    }
}

impl<'o> ReplyNotFound<'o> for Lookup {
    fn not_found(reply: Reply<'o, Self>) -> Done<'o> {
        reply.fail(Errno::ENOENT)
    }
}

impl<'o> ReplyKnown<'o> for Lookup {}

impl<'o> ReplyFound<'o> for Lookup {
    fn not_found_for(reply: Reply<'o, Self>, ttl: Ttl) -> Done<'o> {
        reply.single(&make_entry(
            (Ino::NULL, ttl),
            (Zeroable::zeroed(), Ttl::NULL),
        ))
    }
}

impl<'o> RequestHandle<'o> for Readdir {
    fn handle(request: &Request<'o, Self>) -> u64 {
        readdir_read_in(request).fh
    }
}

impl<'o> RequestOffset<'o> for Readdir {
    fn offset(request: &Request<'o, Self>) -> u64 {
        readdir_read_in(request).offset
    }
}

impl<'o> RequestSize<'o> for Readdir {
    fn size(request: &Request<'o, Self>) -> u32 {
        readdir_read_in(request).size
    }
}

impl<'o, B> ReplyBuffered<'o, B> for Readdir
where
    B: BufMut + AsRef<[u8]>,
{
    type Buffered = BufferedReaddir<B>;

    fn buffered(reply: Reply<'o, Self>, buffer: B) -> Reply<'o, Self::Buffered> {
        assert!(buffer.as_ref().is_empty());

        let ReaddirState {
            max_read,
            is_plus,
            buffer: (),
        } = reply.state;

        Reply {
            session: reply.session,
            unique: reply.unique,
            state: ReaddirState {
                max_read,
                is_plus,
                buffer,
            },
        }
    }
}

impl<'o, B: BufMut + AsRef<[u8]>> ReplyEntries<'o> for BufferedReaddir<B> {
    fn entry(mut reply: Reply<'o, Self>, entry: Entry<impl Known>) -> Interruptible<'o, Self, ()> {
        let entry_header_len = if reply.state.is_plus {
            std::mem::size_of::<proto::DirentPlus>()
        } else {
            std::mem::size_of::<proto::Dirent>()
        };

        let name = entry.name.as_bytes();
        let padding_len = dirent_pad_bytes(entry_header_len + name.len());

        let buffer = &mut reply.state.buffer;
        let remaining = buffer
            .remaining_mut()
            .min(reply.state.max_read - buffer.as_ref().len());

        let record_len = entry_header_len + name.len() + padding_len;
        if remaining < record_len {
            if buffer.as_ref().is_empty() {
                log::error!("Buffer for readdir req #{} is too small", reply.unique);
                return Interruptible::Interrupted(reply.fail(Errno::ENOBUFS));
            }

            return Interruptible::Interrupted(reply.end());
        }

        let inode = entry.inode.inode();
        let entry_type = match inode.inode_type() {
            EntryType::Fifo => SFlag::S_IFIFO,
            EntryType::CharacterDevice => SFlag::S_IFCHR,
            EntryType::Directory => SFlag::S_IFDIR,
            EntryType::BlockDevice => SFlag::S_IFBLK,
            EntryType::File => SFlag::S_IFREG,
            EntryType::Symlink => SFlag::S_IFLNK,
            EntryType::Socket => SFlag::S_IFSOCK,
        };

        let ino = inode.ino();
        let dirent = proto::Dirent {
            ino: ino.as_raw(),
            off: entry.offset,
            namelen: name.len().try_into().unwrap(),
            entry_type: entry_type.bits() >> 12,
        };

        enum Ent {
            Dirent(proto::Dirent),
            DirentPlus(proto::DirentPlus),
        }

        let ent = if reply.state.is_plus {
            let (attrs, attrs_ttl) = inode.attrs();
            let attrs = attrs.finish(inode);
            let entry_out = make_entry((ino, entry.ttl), (attrs, attrs_ttl));

            if name != ".".as_bytes() && name != "..".as_bytes() {
                entry.inode.unveil();
            }

            Ent::DirentPlus(proto::DirentPlus { entry_out, dirent })
        } else {
            Ent::Dirent(dirent)
        };

        let entry_header = match &ent {
            Ent::Dirent(dirent) => bytes_of(dirent),
            Ent::DirentPlus(dirent_plus) => bytes_of(dirent_plus),
        };

        buffer.put_slice(entry_header);
        buffer.put_slice(name);
        buffer.put_slice(&[0; 7][..padding_len]);

        if remaining - record_len >= entry_header.len() + (1 << proto::DIRENT_ALIGNMENT_BITS) {
            Interruptible::Completed(reply, ())
        } else {
            Interruptible::Interrupted(reply.end())
        }
    }

    fn end(reply: Reply<'o, Self>) -> Done<'o> {
        reply.inner(|reply| reply.state.buffer.as_ref())
    }
}

impl<'o> FromRequest<'o, Readdir> for ReaddirState<()> {
    fn from_request(request: &Request<'o, Readdir>) -> Self {
        ReaddirState {
            max_read: request.size() as usize,
            is_plus: matches!(request.body, proto::OpcodeSelect::Match(_)),
            buffer: (),
        }
    }
}

fn dirent_pad_bytes(entry_len: usize) -> usize {
    const ALIGN_MASK: usize = (1 << proto::DIRENT_ALIGNMENT_BITS) - 1;
    ((entry_len + ALIGN_MASK) & !ALIGN_MASK) - entry_len
}

fn readdir_read_in<'a>(request: &'a Request<'_, Readdir>) -> &'a proto::ReadIn {
    use proto::OpcodeSelect::*;

    match &request.body {
        Match(readdir_plus) => &readdir_plus.read_in,
        Alt(readdir) => &readdir.read_in,
    }
}
