// Based on libfuse/include/fuse_kernel.h

use bitflags::bitflags;
use bytemuck::{self, try_cast_slice, try_from_bytes, Pod};
use bytemuck_derive::{Pod, Zeroable};
use num_enum::TryFromPrimitive;
use std::{convert::TryFrom, ffi::CStr, fmt};

use crate::{util::display_or, FuseError, FuseResult};

pub const ROOT_ID: u64 = 1;
pub const MAJOR_VERSION: u32 = 7;
pub const TARGET_MINOR_VERSION: u32 = 32;
pub const REQUIRED_MINOR_VERSION: u32 = 31;

pub const MIN_READ_SIZE: usize = 8192;
pub const DIRENT_ALIGNMENT_BITS: usize = 3;

pub trait Structured<'o>: Sized {
    fn split_from(bytes: &'o [u8], header: &InHeader, last: bool) -> FuseResult<(Self, &'o [u8])>;

    fn toplevel_from(bytes: &'o [u8], header: &InHeader) -> FuseResult<Self> {
        match Self::split_from(bytes, header, true)? {
            (ok, end) if end.is_empty() => Ok(ok),
            _ => Err(FuseError::BadLength),
        }
    }
}

pub enum OpcodeSelect<L, R, const OP: u32> {
    Match(L),
    Alt(R),
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct InHeader {
    pub len: u32,
    pub opcode: u32,
    pub unique: u64,
    pub ino: u64,
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct OutHeader {
    pub len: u32,
    pub error: i32,
    pub unique: u64,
}

#[derive(TryFromPrimitive, Copy, Clone, Debug)]
#[repr(u32)]
pub enum Opcode {
    Lookup = 1,
    Forget = 2,
    Getattr = 3,
    Setattr = 4,
    Readlink = 5,
    Symlink = 6,
    Mknod = 8,
    Mkdir = 9,
    Unlink = 10,
    Rmdir = 11,
    Rename = 12,
    Link = 13,
    Open = 14,
    Read = 15,
    Write = 16,
    Statfs = 17,
    Release = 18,
    Fsync = 20,
    Setxattr = 21,
    Getxattr = 22,
    Listxattr = 23,
    Removexattr = 24,
    Flush = 25,
    Init = 26,
    Opendir = 27,
    Readdir = 28,
    Releasedir = 29,
    Fsyncdir = 30,
    Getlk = 31,
    Setlk = 32,
    Setlkw = 33,
    Access = 34,
    Create = 35,
    Interrupt = 36,
    Bmap = 37,
    Destroy = 38,
    Ioctl = 39,
    Poll = 40,
    NotifyReply = 41,
    BatchForget = 42,
    Fallocate = 43,
    ReaddirPlus = 44,
    Rename2 = 45,
    Lseek = 46,
    CopyFileRange = 47,
}

#[derive(TryFromPrimitive, Copy, Clone)]
#[repr(i32)]
pub enum NotifyCode {
    Poll = 1,
    InvalInode = 2,
    InvalEntry = 3,
    Store = 4,
    Retrieve = 5,
    Delete = 6,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct Attrs {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
    pub atimensec: u32,
    pub mtimensec: u32,
    pub ctimensec: u32,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct FileLock {
    pub start: u64,
    pub end: u64,
    pub lock_type: u32,
    pub pid: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct EntryOut {
    pub nodeid: u64,
    pub generation: u64,
    pub entry_valid: u64,
    pub attr_valid: u64,
    pub entry_valid_nsec: u32,
    pub attr_valid_nsec: u32,
    pub attr: Attrs,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct Dirent {
    pub ino: u64,
    pub off: u64,
    pub namelen: u32,
    pub entry_type: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct DirentPlus {
    pub entry_out: EntryOut,
    pub dirent: Dirent,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct ForgetIn {
    pub nlookup: u64,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct GetattrIn {
    pub flags: u32,
    pub dummy: u32,
    pub fh: u64,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct AttrOut {
    pub attr_valid: u64,
    pub attr_valid_nsec: u32,
    pub dummy: u32,
    pub attr: Attrs,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct SetattrIn {
    pub valid: u32,
    pub padding: u32,
    pub fh: u64,
    pub size: u64,
    pub lock_owner: u64,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
    pub atimensec: u32,
    pub mtimensec: u32,
    pub ctimensec: u32,
    pub mode: u32,
    pub unused: u32,
    pub uid: u32,
    pub gid: u32,
    pub unused2: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct MknodIn {
    pub mode: u32,
    pub device: u32,
    pub umask: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct MkdirIn {
    pub mode: u32,
    pub umask: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct RenameIn {
    pub new_dir: u64,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct LinkIn {
    pub old_ino: u64,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct OpenIn {
    pub flags: u32,
    pub unused: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct OpenOut {
    pub fh: u64,
    pub open_flags: u32,
    pub padding: u32,
}

bitflags! {
    pub struct OpenOutFlags: u32 {
        const DIRECT_IO   = 1 << 0;
        const KEEP_CACHE  = 1 << 1;
        const NONSEEKABLE = 1 << 2;
        const CACHE_DIR   = 1 << 3;
        const STREAM      = 1 << 4;
    }
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct ReadIn {
    pub fh: u64,
    pub offset: u64,
    pub size: u32,
    pub read_flags: u32,
    pub lock_owner: u64,
    pub flags: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct WriteIn {
    pub fh: u64,
    pub offset: u64,
    pub size: u32,
    pub write_flags: u32,
    pub lock_owner: u64,
    pub flags: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct WriteOut {
    pub size: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct StatfsOut {
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub files: u64,
    pub ffree: u64,
    pub bsize: u32,
    pub namelen: u32,
    pub frsize: u32,
    pub padding: u32,
    pub spare: [u32; 6],
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct ReleaseIn {
    pub fh: u64,
    pub flags: u32,
    pub release_flags: u32,
    pub lock_owner: u64,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct FsyncIn {
    pub fh: u64,
    pub fsync_flags: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct SetxattrIn {
    pub size: u32,
    pub flags: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct GetxattrIn {
    pub size: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct GetxattrOut {
    pub size: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct ListxattrIn {
    pub getxattr_in: GetxattrIn,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct ListxattrOut {
    pub getxattr_out: GetxattrOut,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct FlushIn {
    pub fh: u64,
    pub unused: u32,
    pub padding: u32,
    pub lock_owner: u64,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct InitIn {
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub flags: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct InitOut {
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub flags: u32,
    pub max_background: u16,
    pub congestion_threshold: u16,
    pub max_write: u32,
    pub time_gran: u32,
    pub max_pages: u16,
    pub padding: u16,
    pub unused: [u32; 8],
}

bitflags! {
    pub struct InitFlags: u32 {
        const ASYNC_READ          = 1 << 0;
        const POSIX_LOCKS         = 1 << 1;
        const FILE_OPS            = 1 << 2;
        const ATOMIC_O_TRUNC      = 1 << 3;
        const EXPORT_SUPPORT      = 1 << 4;
        const BIG_WRITES          = 1 << 5;
        const DONT_MASK           = 1 << 6;
        const SPLICE_WRITE        = 1 << 7;
        const SPLICE_MOVE         = 1 << 8;
        const SPLICE_READ         = 1 << 9;
        const FLOCK_LOCKS         = 1 << 10;
        const HAS_IOCTL_DIR       = 1 << 11;
        const AUTO_INVAL_DATA     = 1 << 12;
        const DO_READDIRPLUS      = 1 << 13;
        const READDIRPLUS_AUTO    = 1 << 14;
        const ASYNC_DIO           = 1 << 15;
        const WRITEBACK_CACHE     = 1 << 16;
        const NO_OPEN_SUPPOR      = 1 << 17;
        const PARALLEL_DIROPS     = 1 << 18;
        const HANDLE_KILLPRIV     = 1 << 19;
        const POSIX_ACL           = 1 << 20;
        const ABORT_ERROR         = 1 << 21;
        const MAX_PAGES           = 1 << 22;
        const CACHE_SYMLINKS      = 1 << 23;
        const NO_OPENDIR_SUPPORT  = 1 << 24;
        const EXPLICIT_INVAL_DATA = 1 << 25;
    }
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct OpendirIn {
    pub open_in: OpenIn,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct ReaddirIn {
    pub read_in: ReadIn,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct ReleasedirIn {
    pub release_in: ReleaseIn,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct FsyncdirIn {
    pub fsync_in: FsyncIn,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct LkIn {
    pub fh: u64,
    pub owner: u64,
    pub lock: FileLock,
    pub lock_flags: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct GetlkIn {
    pub lk_in: LkIn,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct SetlkIn {
    pub lk_in: LkIn,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct SetlkwIn {
    pub lk_in: LkIn,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct AccessIn {
    pub mask: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct CreateIn {
    pub flags: u32,
    pub mode: u32,
    pub umask: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct InterruptIn {
    pub unique: u64,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct BmapIn {
    pub block: u64,
    pub block_size: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct IoctlIn {
    pub fh: u64,
    pub flags: u32,
    pub cmd: u32,
    pub arg: u64,
    pub in_size: u32,
    pub out_size: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct PollIn {
    pub fh: u64,
    pub kh: u64,
    pub flags: u32,
    pub events: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct ForgetOne {
    pub ino: u64,
    pub nlookup: u64,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct BatchForgetIn {
    pub count: u32,
    pub dummy: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct FallocateIn {
    pub fh: u64,
    pub offset: u64,
    pub length: u64,
    pub mode: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct ReaddirPlusIn {
    pub read_in: ReadIn,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct Rename2In {
    pub new_dir: u64,
    pub flags: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct LseekIn {
    pub fh: u64,
    pub offset: u64,
    pub whence: u32,
    pub padding: u32,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct CopyFileRangeIn {
    pub fh_in: u64,
    pub off_in: u64,
    pub nodeid_out: u64,
    pub fh_out: u64,
    pub off_out: u64,
    pub len: u64,
    pub flags: u64,
}

impl<'o> Structured<'o> for () {
    fn split_from(bytes: &'o [u8], _: &InHeader, _last: bool) -> FuseResult<(Self, &'o [u8])> {
        Ok(((), bytes))
    }
}

impl<'o, T, U> Structured<'o> for (T, U)
where
    T: Structured<'o>,
    U: Structured<'o>,
{
    fn split_from(bytes: &'o [u8], header: &InHeader, last: bool) -> FuseResult<(Self, &'o [u8])> {
        let (first, bytes) = T::split_from(bytes, header, false)?;
        let (second, end) = U::split_from(bytes, header, last)?;
        Ok(((first, second), end))
    }
}

impl<'o, T, U, V> Structured<'o> for (T, U, V)
where
    T: Structured<'o>,
    U: Structured<'o>,
    V: Structured<'o>,
{
    fn split_from(bytes: &'o [u8], header: &InHeader, last: bool) -> FuseResult<(Self, &'o [u8])> {
        let (first, bytes) = T::split_from(bytes, header, false)?;
        let ((second, third), end) = <(U, V)>::split_from(bytes, header, last)?;
        Ok(((first, second, third), end))
    }
}

impl<'o, T: Pod> Structured<'o> for &'o T {
    fn split_from(bytes: &'o [u8], _: &InHeader, _last: bool) -> FuseResult<(Self, &'o [u8])> {
        let (bytes, next_bytes) = bytes.split_at(bytes.len().min(std::mem::size_of::<T>()));
        match try_from_bytes(bytes) {
            Ok(t) => Ok((t, next_bytes)),
            Err(_) => Err(FuseError::Truncated),
        }
    }
}

impl<'o, T: Pod> Structured<'o> for &'o [T] {
    fn split_from(bytes: &'o [u8], _header: &InHeader, last: bool) -> FuseResult<(Self, &'o [u8])> {
        if !last {
            unimplemented!();
        }

        match try_cast_slice(bytes) {
            Ok(slice) => Ok((slice, &[])),
            Err(_) => Err(FuseError::Truncated),
        }
    }
}

impl<'o> Structured<'o> for &'o CStr {
    fn split_from(bytes: &'o [u8], _header: &InHeader, last: bool) -> FuseResult<(Self, &'o [u8])> {
        let (cstr, after_cstr): (&[u8], &[u8]) = if last {
            (bytes, &[])
        } else {
            match bytes.iter().position(|byte| *byte == b'\0') {
                Some(nul) => bytes.split_at(nul + 1),
                None => return Err(FuseError::Truncated),
            }
        };

        let cstr = CStr::from_bytes_with_nul(cstr).map_err(|_| FuseError::BadLength)?;
        Ok((cstr, after_cstr))
    }
}

impl<'o, L, R, const OP: u32> Structured<'o> for OpcodeSelect<L, R, OP>
where
    L: Structured<'o>,
    R: Structured<'o>,
{
    fn split_from(bytes: &'o [u8], header: &InHeader, last: bool) -> FuseResult<(Self, &'o [u8])> {
        if header.opcode == OP {
            L::split_from(bytes, header, last).map(|(l, end)| (OpcodeSelect::Match(l), end))
        } else {
            R::split_from(bytes, header, last).map(|(r, end)| (OpcodeSelect::Alt(r), end))
        }
    }
}

impl InHeader {
    pub fn from_bytes(bytes: &[u8]) -> FuseResult<(Self, Opcode)> {
        let header_bytes = &bytes[..bytes.len().min(std::mem::size_of::<InHeader>())];
        let header = try_from_bytes::<InHeader>(header_bytes).map_err(|_| FuseError::Truncated)?;

        if header.len as usize != bytes.len() {
            return Err(FuseError::BadLength);
        }

        let opcode = match Opcode::try_from(header.opcode) {
            Ok(opcode) => opcode,
            Err(_) => return Err(FuseError::BadOpcode),
        };

        Ok((*header, opcode))
    }
}

impl fmt::Display for InHeader {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let opcode = display_or(Opcode::try_from(self.opcode).ok(), "bad opcode");

        write!(
            fmt,
            "<{}> #{} len={} ino={} uid={} gid={} pid={}",
            opcode, self.unique, self.len, self.ino, self.uid, self.gid, self.pid
        )
    }
}

impl fmt::Display for Opcode {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "{:?} ({})", self, *self as u32)
    }
}
