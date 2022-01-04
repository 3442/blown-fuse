use std::{
    convert::Infallible,
    ffi::{CStr, OsStr},
    marker::PhantomData,
    os::unix::ffi::OsStrExt,
};

use crate::fuse::{
    io::{Entry, EntryType, Interruptible, Known, Stat},
    private_trait::Sealed,
    Done, Operation, Reply, Request,
};

use super::{c_to_os, FromRequest};
use crate::{proto, Errno, Ino, Ttl};
use bytemuck::{bytes_of, Zeroable};
use bytes::BufMut;
use nix::sys::stat::SFlag;

pub enum Lookup {}
pub enum Readdir {}
pub struct BufferedReaddir<B>(Infallible, PhantomData<B>);

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
    type ReplyTail = ();
}

impl<'o> Operation<'o> for Readdir {
    type RequestBody = proto::OpcodeSelect<
        &'o proto::ReaddirPlusIn,
        &'o proto::ReaddirIn,
        { proto::Opcode::ReaddirPlus as u32 },
    >;

    type ReplyTail = ReaddirState<()>;
}

impl<'o, B> Operation<'o> for BufferedReaddir<B> {
    type RequestBody = (); // Never actually created
    type ReplyTail = ReaddirState<B>;
}

impl<'o> Request<'o, Lookup> {
    /// Returns the name of the entry being looked up in this directory.
    pub fn name(&self) -> &OsStr {
        c_to_os(self.body)
    }
}

impl<'o> Reply<'o, Lookup> {
    /// The requested entry was found. The FUSE client will become aware of the found inode if
    /// it wasn't before. This result may be cached by the client for up to the given TTL.
    pub fn found(self, entry: impl Known, ttl: Ttl) -> Done<'o> {
        let (attrs, attrs_ttl) = entry.inode().attrs();
        let attrs = attrs.finish(entry.inode());

        let done = self.single(&make_entry((entry.inode().ino(), ttl), (attrs, attrs_ttl)));
        entry.unveil();

        done
    }

    /// The requested entry was not found in this directory. The FUSE clint may include this
    /// response in negative cache for up to the given TTL.
    pub fn not_found(self, ttl: Ttl) -> Done<'o> {
        self.single(&make_entry(
            (Ino::NULL, ttl),
            (Zeroable::zeroed(), Ttl::NULL),
        ))
    }

    /// The requested entry was not found in this directory, but unlike [`Reply::not_found()`]
    /// this does not report back a TTL to the FUSE client. The client should not cache the
    /// response.
    pub fn not_found_uncached(self) -> Done<'o> {
        self.fail(Errno::ENOENT)
    }
}

impl<'o> Request<'o, Readdir> {
    pub fn handle(&self) -> u64 {
        self.read_in().fh
    }

    /// Returns the base offset in the directory stream to read from.
    pub fn offset(&self) -> u64 {
        self.read_in().offset
    }

    pub fn size(&self) -> u32 {
        self.read_in().size
    }

    fn read_in(&self) -> &proto::ReadIn {
        use proto::OpcodeSelect::*;

        match &self.body {
            Match(readdir_plus) => &readdir_plus.read_in,
            Alt(readdir) => &readdir.read_in,
        }
    }
}

impl<'o> Reply<'o, Readdir> {
    pub fn buffered<B>(self, buffer: B) -> Reply<'o, BufferedReaddir<B>>
    where
        B: BufMut + AsRef<[u8]>,
    {
        assert!(buffer.as_ref().is_empty());

        let ReaddirState {
            max_read,
            is_plus,
            buffer: (),
        } = self.tail;

        Reply {
            session: self.session,
            unique: self.unique,
            tail: ReaddirState {
                max_read,
                is_plus,
                buffer,
            },
        }
    }
}

impl<'o, B: BufMut + AsRef<[u8]>> Reply<'o, BufferedReaddir<B>> {
    pub fn entry(mut self, entry: Entry<impl Known>) -> Interruptible<'o, BufferedReaddir<B>, ()> {
        let entry_header_len = if self.tail.is_plus {
            std::mem::size_of::<proto::DirentPlus>()
        } else {
            std::mem::size_of::<proto::Dirent>()
        };

        let name = entry.name.as_bytes();
        let padding_len = dirent_pad_bytes(entry_header_len + name.len());

        let buffer = &mut self.tail.buffer;
        let remaining = buffer
            .remaining_mut()
            .min(self.tail.max_read - buffer.as_ref().len());

        let record_len = entry_header_len + name.len() + padding_len;
        if remaining < record_len {
            if buffer.as_ref().is_empty() {
                log::error!("Buffer for readdir req #{} is too small", self.unique);
                return Interruptible::Interrupted(self.fail(Errno::ENOBUFS));
            }

            return Interruptible::Interrupted(self.end());
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

        let ent = if self.tail.is_plus {
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
            Interruptible::Completed(self, ())
        } else {
            Interruptible::Interrupted(self.end())
        }
    }

    pub fn end(self) -> Done<'o> {
        self.inner(|this| this.tail.buffer.as_ref())
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

fn make_entry(
    (Ino(ino), entry_ttl): (Ino, Ttl),
    (attrs, attr_ttl): (proto::Attrs, Ttl),
) -> proto::EntryOut {
    proto::EntryOut {
        nodeid: ino,
        generation: 0, //TODO
        entry_valid: entry_ttl.seconds,
        attr_valid: attr_ttl.seconds,
        entry_valid_nsec: entry_ttl.nanoseconds,
        attr_valid_nsec: attr_ttl.nanoseconds,
        attr: attrs,
    }
}

fn dirent_pad_bytes(entry_len: usize) -> usize {
    const ALIGN_MASK: usize = (1 << proto::DIRENT_ALIGNMENT_BITS) - 1;
    ((entry_len + ALIGN_MASK) & !ALIGN_MASK) - entry_len
}
