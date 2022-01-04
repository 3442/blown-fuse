use crate::{proto, util::page_size};
use crate::fuse::{io::FsInfo, private_trait::Sealed, Done, Operation, Reply};

pub enum Init {}
pub enum Statfs {}

pub struct InitState {
    pub(crate) kernel_flags: proto::InitFlags,
    pub(crate) buffer_pages: usize,
}

impl Sealed for Init {}
impl Sealed for Statfs {}

impl<'o> Operation<'o> for Init {
    type RequestBody = &'o proto::InitIn;
    type ReplyTail = InitState;
}

impl<'o> Operation<'o> for Statfs {
    type RequestBody = ();
    type ReplyTail = ();
}

impl<'o> Reply<'o, Init> {
    pub fn ok(self) -> Done<'o> {
        let InitState {
            kernel_flags,
            buffer_pages,
        } = self.tail;

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

impl<'o> Reply<'o, Statfs> {
    /// Replies with filesystem statistics.
    pub fn info(self, statfs: &FsInfo) -> Done<'o> {
        self.single(&proto::StatfsOut::from(*statfs))
    }
}
