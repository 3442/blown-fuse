use bytemuck::{bytes_of, Pod, Zeroable};

use std::{
    ffi::{CStr, OsStr},
    os::unix::ffi::OsStrExt,
};

use crate::{
    proto,
    util::{page_size, OutputChain},
    Errno, Ino, TimeToLive,
};

use super::{
    io::{AccessFlags, Entry, FsInfo, Interruptible, Known},
    Done, Operation, Reply, Request,
};

macro_rules! op {
    { $name:ident $operation:tt } => {
        pub struct $name(std::convert::Infallible);

        impl super::private_trait::Sealed for $name {}
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
        pub fn found(self, entry: impl Known, ttl: TimeToLive) -> Done<'o> {
            let (attrs, attrs_ttl) = entry.attrs();
            let attrs = attrs.finish(&entry);

            let done = self.single(&make_entry((entry.ino(), ttl), (attrs, attrs_ttl)));
            entry.unveil();

            done
        }

        /// The requested entry was not found in this directory. The FUSE clint may include this
        /// response in negative cache for up to the given TTL.
        pub fn not_found(self, ttl: TimeToLive) -> Done<'o> {
            self.single(&make_entry((Ino::NULL, ttl), (Zeroable::zeroed(), Default::default())))
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
            &'o proto::ForgetOne,
            { proto::Opcode::BatchForget as u32 },
        >;

        type ReplyTail = ();
    }

    impl Reply {
        pub fn ok(self) -> Done<'o> {
            // No reply for forget requests
            Done::done()
        }
    }
}

op! {
    Getattr {
        type RequestBody = &'o proto::GetattrIn;
        type ReplyTail = ();
    }

    impl Reply {
        pub fn known(self, inode: &impl Known) -> Done<'o> {
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
        pub fn target(self, target: &OsStr) -> Done<'o> {
            self.chain(OutputChain::tail(&[target.as_bytes()]))
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
        type ReplyTail = state::OpenFlags;
    }

    impl Reply {
        pub fn force_direct_io(&mut self) {
            self.tail.0 |= proto::OpenOutFlags::DIRECT_IO;
        }

        /// The inode may now be accessed.
        pub fn ok(self) -> Done<'o> {
            self.ok_with_handle(0)
        }

        fn ok_with_handle(self, handle: u64) -> Done<'o> {
            let open_flags = self.tail.0.bits();

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
        type RequestBody = ();
        type ReplyTail = ();
    }
}

op! {
    Write {
        type RequestBody = &'o proto::WriteIn;
        type ReplyTail = ();
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

                let supported = InitFlags::PARALLEL_DIROPS
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
        pub fn info(self, statfs: FsInfo) -> Done<'o> {
            let statfs: proto::StatfsOut = statfs.into();
            self.single(&statfs)
        }
    }
}

op! {
    Release {
        type RequestBody = &'o proto::ReleaseIn;
        type ReplyTail = ();
    }
}

op! {
    Opendir {
        type RequestBody = &'o proto::OpendirIn;
        type ReplyTail = ();
    }
}

op! {
    Readdir {
        type RequestBody = &'o proto::ReaddirIn;
        type ReplyTail = ();
    }

    impl Request {
        /// Returns the base offset in the directory stream to read from.
        pub fn offset(&self) -> u64 {
            self.body.read_in.offset
        }
    }

    impl Reply {
        pub fn entry<N>(self, inode: Entry<N, impl Known>) -> Interruptible<'o, Readdir, ()>
        where
            N: AsRef<OsStr>,
        {
            todo!()
        }

        pub fn end(self) -> Done<'o> {
            todo!()
        }
    }
}

op! {
    Releasedir {
        type RequestBody = &'o proto::ReleasedirIn;
        type ReplyTail = ();
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

op! {
    Destroy {
        type RequestBody = ();
        type ReplyTail = ();
    }
}

pub(crate) mod state {
    use crate::proto;

    pub struct Init {
        pub kernel_flags: proto::InitFlags,
        pub buffer_pages: usize,
    }

    #[derive(Copy, Clone)]
    pub struct OpenFlags(pub proto::OpenOutFlags);

    impl Default for OpenFlags {
        fn default() -> Self {
            OpenFlags(proto::OpenOutFlags::empty())
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

    fn chain(self, chain: OutputChain<'_>) -> Done<'o> {
        let result = self.session.ok(self.unique, chain);
        self.finish(result)
    }
}

fn c_to_os(string: &CStr) -> &OsStr {
    OsStr::from_bytes(string.to_bytes())
}

fn make_entry(
    (Ino(ino), entry_ttl): (Ino, TimeToLive),
    (attrs, attr_ttl): (proto::Attrs, TimeToLive),
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
