use bytemuck::{bytes_of, Pod, Zeroable};
use futures_util::stream::{Stream, StreamExt, TryStreamExt};
use nix::sys::stat::SFlag;

use std::{
    borrow::Borrow,
    ffi::{CStr, OsStr},
    os::unix::ffi::OsStrExt,
};

use crate::{proto, util::OutputChain, Errno, Ino, TimeToLive};

use super::{
    fs::{Fuse, Inode},
    io::{AccessFlags, Entry, EntryType, FsInfo},
    session, Done, Operation, Reply, Request,
};

macro_rules! op {
    { $name:ident $operation:tt $(,)+ } => {
        pub struct $name(());

        impl<'o, Fs: Fuse> Operation<'o, Fs> for $name $operation
    };

    { $name:ident $operation:tt, Request $request:tt $($next:tt)+ } => {
        impl<'o, Fs: Fuse> Request<'o, Fs, $name> $request

        op! { $name $operation $($next)+ }
    };

    { $name:ident $operation:tt, Reply $reply:tt $($next:tt)+ } => {
        impl<'o, Fs: Fuse> Reply<'o, Fs, $name> $reply

        op! { $name $operation $($next)+ }
    };
}

op! {
    Lookup {
        // name()
        type RequestBody = &'o CStr;
    },

    Request {
        /// Returns the name of the entry being looked up in this directory.
        pub fn name(&self) -> &OsStr {
            c_to_os(self.body)
        }
    },

    Reply {
        /// The requested entry was found and a `Farc` was successfully determined from it. The
        /// FUSE client will become aware of the found inode if it wasn't before. This result may
        /// be cached by the client for up to the given TTL.
        pub fn found(self, entry: &Fs::Farc, ttl: TimeToLive) -> Done<'o> {
            let (attrs, attrs_ttl) = <Fs as Fuse>::Inode::attrs(entry);
            session::unveil(&self.session, entry);

            self.single(&make_entry(
                (<Fs as Fuse>::Inode::ino(entry), ttl),
                (attrs.finish::<Fs>(entry), attrs_ttl),
            ))
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
    },
}

op! {
    Readlink {},

    Reply {
        /// This inode corresponds to a symbolic link pointing to the given target path.
        pub fn target(self, target: &OsStr) -> Done<'o> {
            self.chain(OutputChain::tail(&[target.as_bytes()]))
        }

        /// Same as [`Reply::target()`], except that the target path is taken from disjoint
        /// slices. This involves no additional allocation.
        pub fn gather_target(self, target: &[&OsStr]) -> Done<'o> {
            //FIXME: Likely UB
            self.chain(OutputChain::tail(unsafe { std::mem::transmute(target) }))
        }
    },
}

op! {
    Open {
        type RequestBody = &'o proto::OpenIn;
        type ReplyTail = (Ino, proto::OpenOutFlags);
    },

    Reply {
        /// The iinode may now be accessed.
        pub fn ok(self) -> Done<'o> {
            self.ok_with_handle(0)
        }

        /*pub fn tape<R: Tape<Fuse = Fs>>(self, reel: R) -> Done<'o> {
            let (ino, _) = self.tail;
            self.ok_with_handle(session::allocate_handle(&self.session, ino, reel))
        }*/

        fn ok_with_handle(self, handle: u64) -> Done<'o> {
            let (_, flags) = self.tail;
            self.single(&proto::OpenOut {
                fh: handle,
                open_flags: flags.bits(),
                padding: Default::default(),
            })
        }
    },
}

op! { Read {}, }
/*op! {
    Read {
        type RequestBody = &'o proto::ReadIn;
        type ReplyTail = &'o mut OutputBytes<'o>;
    },

    Request {
        pub fn offset(&self) -> u64 {
            self.body.offset
        }

        pub fn size(&self) -> u32 {
            self.body.size
        }
    },

    Reply {
        pub fn remaining(&self) -> u64 {
            self.tail.remaining()
        }

        pub fn end(self) -> Done<'o> {
            if self.tail.ready() {
                self.chain(OutputChain::tail(self.tail.segments()))
            } else {
                // The read() handler will be invoked again with same OutputBytes
                self.done()
            }
        }

        pub fn hole(self, size: u64) -> Result<Self, Done<'o>> {
            self.tail
        }

        pub fn copy(self, data: &[u8]) -> Result<Self, Done<'o>> {
            self.self_or_done(self.tail.copy(data))
        }

        pub fn put(self, data: &'o [u8]) -> Result<Self, Done<'o>> {
            self.self_or_done(self.tail.put(data))
        }

        pub fn gather(self, data: &'o [&'o [u8]]) -> Result<Self, Done<'o>> {
            self.self_or_done(self.tail.gather(data))
        }

        fn self_or_done(self, capacity: OutputCapacity) -> Result<Self, Done<'o>> {
            match capacity {
                OutputCapacity::Available => Ok(self),
                OutputCapacity::Filled => Err(self.done()),
            }
        }
    },
}*/

op! {
    Write {
        type RequestBody = &'o proto::WriteIn;
    },
}

op! {
    Init {
        type ReplyTail = &'o mut Result<Fs::Farc, i32>;

        fn consume_errno(errno: i32, tail: &mut Self::ReplyTail) {
            **tail = Err(errno);
        }
    },

    Reply {
        /// Server-side initialization succeeded. The provided `Farc` references the filesystem's
        /// root inode.
        pub fn root(self, root: Fs::Farc) -> Done<'o> {
            *self.tail = Ok(root);
            self.done()
        }
    },
}

op! {
    Statfs {},

    Reply {
        /// Replies with filesystem statistics.
        pub fn info(self, statfs: FsInfo) -> Done<'o> {
            let statfs: proto::StatfsOut = statfs.into();
            self.single(&statfs)
        }
    },
}

op! {
    Opendir {
        type RequestBody = &'o proto::OpendirIn;
    },
}

op! {
    Readdir {
        type RequestBody = &'o proto::ReaddirIn;
    },

    Request {
        /// Returns the base offset in the directory stream to read from.
        pub fn offset(&self) -> u64 {
            self.body.read_in.offset
        }
    },

    Reply {
        pub fn try_iter<'a, I, E, Ref>(
            self,
            mut entries: I,
        ) -> Result<Done<'o>, (Reply<'o, Fs, Readdir>, E)>
        where
            I: Iterator<Item = Result<Entry<'a, Ref>, E>> + Send,
            Ref: Borrow<Fs::Farc>,
        {
            //TODO: This is about as shitty as it gets
            match entries.next().transpose() {
                Ok(Some(entry)) => {
                    let Entry {
                        name,
                        inode,
                        offset,
                        ..
                    } = entry;

                    let inode = inode.borrow();
                    let Ino(ino) = <Fs as Fuse>::Inode::ino(inode);

                    let dirent = proto::Dirent {
                        ino,
                        off: offset,
                        namelen: name.len() as u32,
                        entry_type: (match <Fs as Fuse>::Inode::inode_type(inode) {
                            EntryType::Fifo => SFlag::S_IFIFO,
                            EntryType::CharacterDevice => SFlag::S_IFCHR,
                            EntryType::Directory => SFlag::S_IFDIR,
                            EntryType::BlockDevice => SFlag::S_IFBLK,
                            EntryType::File => SFlag::S_IFREG,
                            EntryType::Symlink => SFlag::S_IFLNK,
                            EntryType::Socket => SFlag::S_IFSOCK,
                        })
                        .bits()
                            >> 12,
                    };

                    let dirent = bytes_of(&dirent);
                    let name = name.as_bytes();

                    let padding = [0; 8];
                    let padding = &padding[..7 - (dirent.len() + name.len() - 1) % 8];

                    Ok(self.chain(OutputChain::tail(&[dirent, name, padding])))
                }

                Err(error) => Err((self, error)),

                Ok(None) => Ok(self.empty()),
            }
        }

        // See rust-lang/rust#61949
        pub async fn try_stream<'a, S, E, Ref>(
            self,
            entries: S,
        ) -> Result<Done<'o>, (Reply<'o, Fs, Readdir>, E)>
        where
            S: Stream<Item = Result<Entry<'a, Ref>, E>> + Send,
            Ref: Borrow<Fs::Farc> + Send,
            E: Send,
        {
            //TODO: This is about as shitty as it gets
            let first = entries.boxed().try_next().await;
            self.try_iter(first.transpose().into_iter())
        }
    },
}

op! {
    Access {
        type RequestBody = &'o proto::AccessIn;
    },

    Request {
        pub fn mask(&self) -> AccessFlags {
            AccessFlags::from_bits_truncate(self.body.mask as i32)
        }
    },

    Reply {
        pub fn ok(self) -> Done<'o> {
            self.empty()
        }

        pub fn permission_denied(self) -> Done<'o> {
            self.fail(Errno::EACCES)
        }
    },
}

impl<'o, Fs: Fuse, O: Operation<'o, Fs>> Reply<'o, Fs, O> {
    fn done(self) -> Done<'o> {
        Done::from_result(Ok(()))
    }

    fn empty(self) -> Done<'o> {
        self.chain(OutputChain::empty())
    }

    fn single<P: Pod>(self, single: &P) -> Done<'o> {
        self.chain(OutputChain::tail(&[bytes_of(single)]))
    }

    fn chain(self, chain: OutputChain<'_>) -> Done<'o> {
        Done::from_result(session::ok(&self.session, self.unique, chain))
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
