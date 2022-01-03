use std::{
    convert::Infallible,
    ffi::{CStr, OsStr},
    marker::PhantomData,
    os::unix::ffi::OsStrExt,
};

use crate::{
    proto,
    util::{page_size, OutputChain},
    Errno, Ino, Ttl,
};

use super::{
    io::{AccessFlags, Entry, EntryType, FsInfo, Interruptible, Known, OpenFlags, Stat},
    private_trait::Sealed,
    Done, Operation, Reply, Request,
};

use bytemuck::{bytes_of, Pod, Zeroable};
use bytes::BufMut;
use nix::sys::stat::SFlag;

pub trait FromRequest<'o, O: Operation<'o>> {
    //TODO: Shouldn't be public
    fn from_request(request: &Request<'o, O>) -> Self;
}

macro_rules! op {
    { $name:ident $operation:tt } => {
        pub struct $name(Infallible);

        impl Sealed for $name {}
        impl<'o> Operation<'o> for $name $operation
    };

    { $name:ident $operation:tt impl Request $request:tt $($next:tt)* } => {
        impl<'o> Request<'o, $name> $request

        op! { $name $operation $($next)* }
    };

    { $name:ident $operation:tt impl Reply $reply:tt $($next:tt)* } => {
        impl<'o> Reply<'o, $name> $reply

        op! { $name $operation $($next)* }
    };
}

op! {
    Any {
        type RequestBody = ();
        type ReplyTail = ();
    }
}

op! {
    Lookup {
        type RequestBody = &'o CStr; // name()
        type ReplyTail = ();
    }

    impl Request {
        /// Returns the name of the entry being looked up in this directory.
        pub fn name(&self) -> &OsStr {
            c_to_os(self.body)
        }
    }

    impl Reply {
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
            self.single(&make_entry((Ino::NULL, ttl), (Zeroable::zeroed(), Ttl::NULL)))
        }

        /// The requested entry was not found in this directory, but unlike [`Reply::not_found()`]
        /// this does not report back a TTL to the FUSE client. The client should not cache the
        /// response.
        pub fn not_found_uncached(self) -> Done<'o> {
            self.fail(Errno::ENOENT)
        }
    }
}

op! {
    Forget {
        type RequestBody = proto::OpcodeSelect<
            (&'o proto::BatchForgetIn, &'o [proto::ForgetOne]),
            &'o proto::ForgetIn,
            { proto::Opcode::BatchForget as u32 },
        >;

        type ReplyTail = ();
    }

    impl Request {
        pub fn forget_list(&self) -> impl '_ + Iterator<Item = (Ino, u64)> {
            use proto::OpcodeSelect::*;

            enum List<'a> {
                Single(Option<(Ino, u64)>),
                Batch(std::slice::Iter<'a, proto::ForgetOne>),
            }

            impl Iterator for List<'_> {
                type Item = (Ino, u64);

                fn next(&mut self) -> Option<Self::Item> {
                    match self {
                        List::Single(single) => single.take(),
                        List::Batch(batch) => {
                            let forget = batch.next()?;
                            Some((Ino(forget.ino), forget.nlookup))
                        }
                    }
                }
            }

            match self.body {
                Match((_, slice)) => List::Batch(slice.iter()),
                Alt(single) => List::Single(Some((self.ino(), single.nlookup))),
            }
        }
    }

    impl Reply {
        pub fn ok(self) -> Done<'o> {
            // No reply for forget requests
            Done::new()
        }
    }
}

op! {
    Getattr {
        type RequestBody = &'o proto::GetattrIn;
        type ReplyTail = ();
    }

    impl Request {
        pub fn handle(&self) -> u64 {
            self.body.fh
        }
    }

    impl Reply {
        pub fn known(self, inode: &impl Stat) -> Done<'o> {
            let (attrs, ttl) = inode.attrs();
            let attrs = attrs.finish(inode);

            self.single(&proto::AttrOut {
                attr_valid: ttl.seconds,
                attr_valid_nsec: ttl.nanoseconds,
                dummy: Default::default(),
                attr: attrs,
            })
        }
    }
}

op! {
    Readlink {
        type RequestBody = ();
        type ReplyTail = ();
    }

    impl Reply {
        /// This inode corresponds to a symbolic link pointing to the given target path.
        pub fn target<T: AsRef<OsStr>>(self, target: T) -> Done<'o> {
            self.chain(OutputChain::tail(&[target.as_ref().as_bytes()]))
        }

        /// Same as [`Reply::target()`], except that the target path is taken from disjoint
        /// slices. This involves no additional allocation.
        pub fn gather_target(self, target: &[&[u8]]) -> Done<'o> {
            self.chain(OutputChain::tail(target))
        }
    }
}

op! {
    Open {
        type RequestBody = &'o proto::OpenIn;
        type ReplyTail = proto::OpenOutFlags;
    }

    impl Request {
        pub fn flags(&self) -> OpenFlags {
            OpenFlags::from_bits_truncate(self.body.flags.try_into().unwrap_or_default())
        }
    }

    impl Reply {
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
}

op! {
    Read {
        type RequestBody = &'o proto::ReadIn;
        type ReplyTail = ();
    }

    impl Request {
        pub fn handle(&self) -> u64 {
            self.body.fh
        }

        pub fn offset(&self) -> u64 {
            self.body.offset
        }

        pub fn size(&self) -> u32 {
            self.body.size
        }
    }

    impl Reply {
        pub fn slice(self, data: &[u8]) -> Done<'o> {
            self.chain(OutputChain::tail(&[data]))
        }
    }
}

op! {
    Write {
        type RequestBody = (&'o proto::WriteIn, &'o [u8]);
        type ReplyTail = state::Write;
    }

    impl Request {
        pub fn handle(&self) -> u64 {
            self.body.0.fh
        }

        pub fn offset(&self) -> u64 {
            self.body.0.offset
        }

        pub fn data(&self) -> &[u8] {
            self.body.1
        }
    }

    impl Reply {
        pub fn all(self) -> Done<'o> {
            let size = self.tail.size;
            self.single(&proto::WriteOut {
                size,
                padding: Default::default(),
            })
        }
    }
}

op! {
    Init {
        type RequestBody = &'o proto::InitIn;
        type ReplyTail = state::Init;
    }

    impl Reply {
        pub fn ok(self) -> Done<'o> {
            let state::Init { kernel_flags, buffer_pages } = self.tail;

            let flags = {
                use proto::InitFlags;

                //TODO: Conditions for these feature flags
                // - Locks
                // - ASYNC_DIO
                // - WRITEBACK_CACHE
                // - NO_OPEN_SUPPORT
                // - HANDLE_KILLPRIV
                // - POSIX_ACL
                // - NO_OPENDIR_SUPPORT
                // - EXPLICIT_INVAL_DATA

                let supported = InitFlags::ASYNC_READ
                    | InitFlags::FILE_OPS
                    | InitFlags::ATOMIC_O_TRUNC
                    | InitFlags::EXPORT_SUPPORT
                    | InitFlags::BIG_WRITES
                    | InitFlags::HAS_IOCTL_DIR
                    | InitFlags::AUTO_INVAL_DATA
                    | InitFlags::DO_READDIRPLUS
                    | InitFlags::READDIRPLUS_AUTO
                    | InitFlags::PARALLEL_DIROPS
                    | InitFlags::ABORT_ERROR
                    | InitFlags::MAX_PAGES
                    | InitFlags::CACHE_SYMLINKS;

                kernel_flags & supported
            };

            let buffer_size = page_size() * buffer_pages;

            // See fs/fuse/dev.c in the kernel source tree for details about max_write
            let max_write = buffer_size - std::mem::size_of::<(proto::InHeader, proto::WriteIn)>();

            self.single(&proto::InitOut {
                major: proto::MAJOR_VERSION,
                minor: proto::TARGET_MINOR_VERSION,
                max_readahead: 0, //TODO
                flags: flags.bits(),
                max_background: 0,       //TODO
                congestion_threshold: 0, //TODO
                max_write: max_write.try_into().unwrap(),
                time_gran: 1, //TODO
                max_pages: buffer_pages.try_into().unwrap(),
                padding: Default::default(),
                unused: Default::default(),
            })
        }
    }
}

op! {
    Statfs {
        type RequestBody = ();
        type ReplyTail = ();
    }

    impl Reply {
        /// Replies with filesystem statistics.
        pub fn info(self, statfs: &FsInfo) -> Done<'o> {
            self.single(&proto::StatfsOut::from(*statfs))
        }
    }
}

op! {
    Release {
        type RequestBody = &'o proto::ReleaseIn;
        type ReplyTail = ();
    }

    impl Request {
        pub fn handle(&self) -> u64 {
            self.body.fh
        }
    }

    impl Reply {
        pub fn ok(self) -> Done<'o> {
            self.empty()
        }
    }
}

op! {
    Setxattr {
        // header, name, value
        type RequestBody = (&'o proto::SetxattrIn, &'o CStr, &'o [u8]);
        type ReplyTail = ();
    }

    //TODO: flags
    impl Request {
        pub fn name(&self) -> &OsStr {
            let (_header, name, _value) = self.body;
            c_to_os(name)
        }

        pub fn value(&self) -> &[u8] {
            let (_header, _name, value) = self.body;
            value
        }
    }

    impl Reply {
        pub fn ok(self) -> Done<'o> {
            self.empty()
        }

        pub fn not_found(self) -> Done<'o> {
            self.fail(Errno::ENODATA)
        }
    }
}

op! {
    Getxattr {
        type RequestBody = (&'o proto::GetxattrIn, &'o CStr);
        type ReplyTail = state::ReadXattr;
    }

    impl Request {
        pub fn size(&self) -> u32 {
            self.body.0.size
        }

        pub fn name(&self) -> &OsStr {
            c_to_os(self.body.1)
        }
    }

    impl Reply {
        pub fn slice(self, value: &[u8]) -> Done<'o> {
            let size = value.len().try_into().expect("Extremely large xattr");
            if self.tail.size == 0 {
                return self.value_size(size);
            } else if self.tail.size < size {
                return self.buffer_too_small();
            }

            self.chain(OutputChain::tail(&[value]))
        }

        pub fn value_size(self, size: u32) -> Done<'o> {
            assert_eq!(self.tail.size, 0);

            self.single(&proto::GetxattrOut {
                size,
                padding: Default::default(),
            })
        }

        pub fn buffer_too_small(self) -> Done<'o> {
            self.fail(Errno::ERANGE)
        }

        pub fn not_found(self) -> Done<'o> {
            self.fail(Errno::ENODATA)
        }
    }
}

op! {
    Listxattr {
        type RequestBody = &'o proto::ListxattrIn;
        type ReplyTail = state::ReadXattr;
    }

    impl Request {
        pub fn size(&self) -> u32 {
            self.body.getxattr_in.size
        }
    }

    impl Reply {
        //TODO: buffered(), gather()

        pub fn value_size(self, size: u32) -> Done<'o> {
            assert_eq!(self.tail.size, 0);

            self.single(&proto::ListxattrOut {
                getxattr_out: proto::GetxattrOut {
                    size,
                    padding: Default::default(),
                },
            })
        }

        pub fn buffer_too_small(self) -> Done<'o> {
            self.fail(Errno::ERANGE)
        }
    }
}

op! {
    Removexattr {
        type RequestBody = &'o CStr;
        type ReplyTail = ();
    }

    impl Request {
        pub fn name(&self) -> &OsStr {
            c_to_os(self.body)
        }
    }

    impl Reply {
        pub fn ok(self) -> Done<'o> {
            self.empty()
        }
    }
}

op! {
    Flush {
        type RequestBody = &'o proto::FlushIn;
        type ReplyTail = ();
    }

    impl Request {
        pub fn handle(&self) -> u64 {
            self.body.fh
        }
    }

    impl Reply {
        pub fn ok(self) -> Done<'o> {
            self.empty()
        }
    }
}

op! {
    Opendir {
        type RequestBody = &'o proto::OpendirIn;
        type ReplyTail = ();
    }

    impl Reply {
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
}

op! {
    Readdir {
        type RequestBody = proto::OpcodeSelect<
            &'o proto::ReaddirPlusIn,
            &'o proto::ReaddirIn,
            { proto::Opcode::ReaddirPlus as u32 },
        >;

        type ReplyTail = state::Readdir<()>;
    }

    impl Request {
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

    impl Reply {
        pub fn buffered<B>(self, buffer: B) -> Reply<'o, BufferedReaddir<B>>
        where
            B: BufMut + AsRef<[u8]>,
        {
            assert!(buffer.as_ref().is_empty());

            let state::Readdir { max_read, is_plus, buffer: (), } = self.tail;

            Reply {
                session: self.session,
                unique: self.unique,
                tail: state::Readdir { max_read, is_plus, buffer, },
            }
        }
    }
}

pub struct BufferedReaddir<B>(Infallible, PhantomData<B>);

impl<B> Sealed for BufferedReaddir<B> {}

impl<'o, B> Operation<'o> for BufferedReaddir<B> {
    type RequestBody = (); // Never actually created
    type ReplyTail = state::Readdir<B>;
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

op! {
    Releasedir {
        type RequestBody = &'o proto::ReleasedirIn;
        type ReplyTail = ();
    }

    impl Request {
        pub fn handle(&self) -> u64 {
            self.body.release_in.fh
        }
    }

    impl Reply {
        pub fn ok(self) -> Done<'o> {
            self.empty()
        }
    }
}

op! {
    Access {
        type RequestBody = &'o proto::AccessIn;
        type ReplyTail = ();
    }

    impl Request {
        pub fn mask(&self) -> AccessFlags {
            AccessFlags::from_bits_truncate(self.body.mask as i32)
        }
    }

    impl Reply {
        pub fn ok(self) -> Done<'o> {
            self.empty()
        }

        pub fn permission_denied(self) -> Done<'o> {
            self.fail(Errno::EACCES)
        }
    }
}

pub(crate) mod state {
    use crate::proto;

    pub struct Init {
        pub kernel_flags: proto::InitFlags,
        pub buffer_pages: usize,
    }

    pub struct Readdir<B> {
        pub(super) max_read: usize,
        pub(super) is_plus: bool,
        pub(super) buffer: B,
    }

    pub struct Write {
        pub(super) size: u32,
    }

    pub struct ReadXattr {
        pub(super) size: u32,
    }
}

impl<'o, O: Operation<'o>> FromRequest<'o, O> for () {
    fn from_request(_request: &Request<'o, O>) -> Self {}
}

impl<'o> FromRequest<'o, Open> for proto::OpenOutFlags {
    fn from_request(_request: &Request<'o, Open>) -> Self {
        proto::OpenOutFlags::empty()
    }
}

impl<'o> FromRequest<'o, Write> for state::Write {
    fn from_request(request: &Request<'o, Write>) -> Self {
        let (body, data) = request.body;

        if body.size as usize != data.len() {
            log::warn!(
                "Write size={} differs from data.len={}",
                body.size,
                data.len()
            );
        }

        state::Write { size: body.size }
    }
}

impl<'o> FromRequest<'o, Readdir> for state::Readdir<()> {
    fn from_request(request: &Request<'o, Readdir>) -> Self {
        state::Readdir {
            max_read: request.size() as usize,
            is_plus: matches!(request.body, proto::OpcodeSelect::Match(_)),
            buffer: (),
        }
    }
}

impl<'o, O: Operation<'o>> Reply<'o, O> {
    fn empty(self) -> Done<'o> {
        self.chain(OutputChain::empty())
    }

    fn single<P: Pod>(self, single: &P) -> Done<'o> {
        self.chain(OutputChain::tail(&[bytes_of(single)]))
    }

    fn inner(self, deref: impl FnOnce(&Self) -> &[u8]) -> Done<'o> {
        let result = self
            .session
            .ok(self.unique, OutputChain::tail(&[deref(&self)]));
        self.finish(result)
    }

    fn chain(self, chain: OutputChain<'_>) -> Done<'o> {
        let result = self.session.ok(self.unique, chain);
        self.finish(result)
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

fn c_to_os(c_str: &CStr) -> &OsStr {
    OsStr::from_bytes(c_str.to_bytes())
}
