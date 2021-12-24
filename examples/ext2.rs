/* Read-only ext2 (rev 1.0) implementation.
 *
 * This is not really async, since the whole backing storage
 * is mmap()ed for simplicity, and then treated as a regular
 * slice (likely unsound, I don't care). Some yields are
 * springled in a few places in order to emulate true async
 * operations.
 *
 * Reference: <https://www.nongnu.org/ext2-doc/ext2.html>
 */

#![feature(arbitrary_self_types)]

#[cfg(target_endian = "big")]
compile_error!("This example assumes a little-endian system");

use std::{
    ffi::{CStr, OsStr},
    fs::File,
    mem::size_of,
    os::unix::{ffi::OsStrExt, io::AsRawFd},
    path::{Path, PathBuf},
    time::{Duration, UNIX_EPOCH},
};

use nix::{
    dir::Type,
    errno::Errno,
    sys::mman::{mmap, MapFlags, ProtFlags},
    sys::stat::Mode,
    unistd::{Gid, Uid},
};

use blown_fuse::{
    fs::Fuse,
    io::{Attrs, Entry, FsInfo},
    mount::{mount_sync, Options},
    ops::{Init, Lookup, Readdir, Readlink, Statfs},
    Done, Ino, Reply, TimeToLive,
};

use async_trait::async_trait;
use bytemuck::{cast_slice, from_bytes, try_from_bytes};
use bytemuck_derive::{Pod, Zeroable};
use clap::{App, Arg};
use futures_util::stream::{self, Stream, TryStreamExt};
use smallvec::SmallVec;
use tokio::{self, runtime::Runtime};
use uuid::Uuid;

const EXT2_ROOT: Ino = Ino(2);

type Op<'o, O> = blown_fuse::Op<'o, Ext2, O>;

#[derive(Copy, Clone)]
struct Farc {
    ino: Ino,
    inode: &'static Inode,
}

impl std::ops::Deref for Farc {
    type Target = Inode;

    fn deref(&self) -> &Self::Target {
        self.inode
    }
}

struct Ext2 {
    backing: &'static [u8],
    superblock: &'static Superblock,
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
struct Superblock {
    s_inodes_count: u32,
    s_blocks_count: u32,
    s_r_blocks_count: u32,
    s_free_blocks_count: u32,
    s_free_inodes_count: u32,
    s_first_data_block: u32,
    s_log_block_size: u32,
    s_log_frag_size: i32,
    s_blocks_per_group: u32,
    s_frags_per_group: u32,
    s_inodes_per_group: u32,
    s_mtime: u32,
    s_wtime: u32,
    s_mnt_count: u16,
    s_max_mnt_count: u16,
    s_magic: u16,
    s_state: u16,
    s_errors: u16,
    s_minor_rev_level: u16,
    s_lastcheck: u32,
    s_checkinterval: u32,
    s_creator_os: u32,
    s_rev_level: u32,
    s_def_resuid: u16,
    s_def_resgid: u16,
    s_first_ino: u32,
    s_inode_size: u16,
    s_block_group_nr: u16,
    s_feature_compat: u32,
    s_feature_incompat: u32,
    s_feature_ro_compat: u32,
    s_uuid: [u8; 16],
    s_volume_name: [u8; 16],
    s_last_mounted: [u8; 64],
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
struct GroupDescriptor {
    bg_block_bitmap: u32,
    bg_inode_bitmap: u32,
    bg_inode_table: u32,
    bg_free_blocks_count: u16,
    bg_free_inodes_count: u16,
    bg_used_dirs_count: u16,
    bg_pad: u16,
    bg_reserved: [u32; 3],
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
struct Inode {
    i_mode: u16,
    i_uid: u16,
    i_size: u32,
    i_atime: u32,
    i_ctime: u32,
    i_mtime: u32,
    i_dtime: u32,
    i_gid: u16,
    i_links_count: u16,
    i_blocks: u32,
    i_flags: u32,
    i_osd1: u32,
    i_block: [u32; 15],
    i_generation: u32,
    i_file_acl: u32,
    i_dir_acl: u32,
    i_faddr: u32,
    i_osd2: [u32; 3],
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
struct LinkedEntry {
    inode: u32,
    rec_len: u16,
    name_len: u8,
    file_type: u8,
}

impl Ext2 {
    fn directory_stream(
        &self,
        inode: Farc,
        start: u64,
    ) -> impl Stream<Item = Result<Entry<'static, Farc>, Errno>> + '_ {
        stream::try_unfold(start, move |mut position| async move {
            loop {
                if position == inode.i_size as u64 {
                    break Ok(None); // End of stream
                }

                let bytes = self.seek_contiguous(&inode, position)?;
                let (header, bytes) = bytes.split_at(size_of::<LinkedEntry>());
                let header: &LinkedEntry = from_bytes(header);

                position += header.rec_len as u64;
                if header.inode == 0 {
                    continue; // Unused entry
                }

                let inode = self.inode(Ino(header.inode as u64))?;
                let name = OsStr::from_bytes(&bytes[..header.name_len as usize]).into();

                let entry = Entry {
                    inode,
                    name,
                    offset: position,
                    ttl: TimeToLive::MAX,
                };

                break Ok(Some((entry, position)));
            }
        })
    }

    fn inode(&self, Ino(ino): Ino) -> Result<Farc, Errno> {
        if ino == 0 {
            log::error!("Attempted to access the null (0) inode");
            return Err(Errno::EIO);
        }

        let index = (ino - 1) as usize;
        let inodes_per_group = self.superblock.s_inodes_per_group as usize;
        let (block, index) = (index / inodes_per_group, index % inodes_per_group);

        let table_base = self.group_descriptors()?[block].bg_inode_table as usize;
        let inode_size = self.superblock.s_inode_size as usize;

        let inodes_per_block = self.block_size() / inode_size;
        let block = table_base + index / inodes_per_block;

        let start = index % inodes_per_block * inode_size;
        let end = start + size_of::<Inode>();

        Ok(Farc {
            ino: Ino(ino),
            inode: from_bytes(&self.block(block)?[start..end]),
        })
    }

    fn seek_contiguous(&self, inode: &Farc, position: u64) -> Result<&'static [u8], Errno> {
        let block_size = self.block_size();
        let position = position as usize;
        let (direct, offset) = (position / block_size, position % block_size);

        let out_of_bounds = || {
            log::error!("Offset {} out of bounds in inode {}", position, inode.ino);
        };

        let chase = |indices: &[usize]| {
            let root: &[u8] = cast_slice(&inode.inode.i_block);
            indices
                .iter()
                .try_fold(root, |ptrs, index| {
                    let ptrs: &[u32] = cast_slice(ptrs);
                    let block = ptrs[*index];

                    if block > 0 {
                        self.block(ptrs[*index] as usize)
                    } else {
                        out_of_bounds();
                        Err(Errno::EIO)
                    }
                })
                .map(|block| &block[offset..])
        };

        const DIRECT_PTRS: usize = 12;

        if direct < DIRECT_PTRS {
            return chase(&[direct]);
        }

        let ptrs_per_block = block_size / size_of::<u32>();
        let (level1, level1_index) = {
            let indirect = direct - DIRECT_PTRS;
            (indirect / ptrs_per_block, indirect % ptrs_per_block)
        };

        if level1 == 0 {
            return chase(&[DIRECT_PTRS, level1_index]);
        }

        let (level2, level2_index) = (level1 / ptrs_per_block, level1 % ptrs_per_block);
        if level2 == 0 {
            return chase(&[DIRECT_PTRS + 1, level2_index, level1_index]);
        }

        let (level3, level3_index) = (level2 / ptrs_per_block, level2 % ptrs_per_block);
        if level3 == 0 {
            chase(&[DIRECT_PTRS + 2, level3_index, level2_index, level1_index])
        } else {
            out_of_bounds();
            Err(Errno::EIO)
        }
    }

    fn group_descriptors(&self) -> Result<&'static [GroupDescriptor], Errno> {
        let start = (self.superblock.s_first_data_block + 1) as usize;
        let groups = (self.superblock.s_blocks_count / self.superblock.s_blocks_per_group) as usize;
        let descriptors_per_block = self.block_size() / size_of::<GroupDescriptor>();
        let table_blocks = (groups + descriptors_per_block - 1) / descriptors_per_block;

        self.blocks(start..start + table_blocks)
            .map(|blocks| &cast_slice(blocks)[..groups])
    }

    fn block(&self, n: usize) -> Result<&'static [u8], Errno> {
        self.blocks(n..n + 1)
    }

    fn blocks(&self, range: std::ops::Range<usize>) -> Result<&'static [u8], Errno> {
        let block_size = self.block_size();
        let (start, end) = (range.start * block_size, range.end * block_size);

        if self.backing.len() >= end {
            Ok(&self.backing[start..end])
        } else {
            log::error!("Bad block range: ({}..{})", range.start, range.end);
            Err(Errno::EIO)
        }
    }

    fn block_size(&self) -> usize {
        1024usize << self.superblock.s_log_block_size
    }
}

#[async_trait]
impl Fuse for Ext2 {
    type Farc = Farc;
    type Inode = Inode;

    async fn init<'o>(&self, reply: Reply<'o, Ext2, Init>) -> Done<'o> {
        let label = &self.superblock.s_volume_name;
        let label = &label[..=label.iter().position(|byte| *byte == b'\0').unwrap_or(0)];
        let label = CStr::from_bytes_with_nul(label)
            .ok()
            .map(|label| {
                let label = label.to_string_lossy();
                if !label.is_empty() {
                    label
                } else {
                    "(empty)".into()
                }
            })
            .unwrap_or("(bad)".into());

        log::info!("UUID: {}", Uuid::from_bytes(self.superblock.s_uuid));
        log::info!("Label: {}", label.escape_debug());

        if let Ok(root) = self.inode(EXT2_ROOT) {
            log::info!("Mounted successfully");
            reply.root(root)
        } else {
            log::error!("Failed to retrieve the root inode");
            reply.io_error()
        }
    }

    async fn statfs<'o>(&self, (_, reply, _): Op<'o, Statfs>) -> Done<'o> {
        let total_blocks = self.superblock.s_blocks_count as u64;
        let free_blocks = self.superblock.s_free_blocks_count as u64;
        let available_blocks = free_blocks - self.superblock.s_r_blocks_count as u64;
        let total_inodes = self.superblock.s_inodes_count as u64;
        let free_inodes = self.superblock.s_free_inodes_count as u64;

        reply.info(
            FsInfo::default()
                .blocks(
                    self.block_size() as u32,
                    total_blocks,
                    free_blocks,
                    available_blocks,
                )
                .inodes(total_inodes, free_inodes)
                .filenames(255),
        )
    }
}

#[async_trait]
impl blown_fuse::fs::Inode for Inode {
    type Fuse = Ext2;

    fn ino(self: &Farc) -> Ino {
        match self.ino {
            Ino::ROOT => EXT2_ROOT,
            EXT2_ROOT => Ino::ROOT,
            ino => ino,
        }
    }

    fn inode_type(self: &Farc) -> Type {
        let inode_type = self.i_mode >> 12;
        match inode_type {
            0x01 => Type::Fifo,
            0x02 => Type::CharacterDevice,
            0x04 => Type::Directory,
            0x06 => Type::BlockDevice,
            0x08 => Type::File,
            0x0A => Type::Symlink,
            0x0C => Type::Socket,

            _ => {
                log::error!("Inode {} has invalid type {:x}", self.ino, inode_type);
                Type::File
            }
        }
    }

    fn attrs(self: &Farc) -> (Attrs, TimeToLive) {
        let (access, modify, change) = {
            let time = |seconds: u32| (UNIX_EPOCH + Duration::from_secs(seconds.into())).into();
            (time(self.i_atime), time(self.i_mtime), time(self.i_ctime))
        };

        let attrs = Attrs::default()
            .size((self.i_dir_acl as u64) << 32 | self.i_size as u64)
            .owner(
                Uid::from_raw(self.i_uid.into()),
                Gid::from_raw(self.i_gid.into()),
            )
            .mode(Mode::from_bits_truncate(self.i_mode.into()))
            .blocks(self.i_blocks.into(), 512)
            .times(access, modify, change)
            .links(self.i_links_count.into());

        (attrs, TimeToLive::MAX)
    }

    async fn lookup<'o>(self: Farc, (request, reply, session): Op<'o, Lookup>) -> Done<'o> {
        let fs = session.fs();
        let name = request.name();

        //TODO: Indexed directories
        let lookup = async move {
            let stream = fs.directory_stream(self, 0);
            tokio::pin!(stream);

            loop {
                match stream.try_next().await? {
                    Some(entry) if entry.name == name => break Ok(Some(entry.inode)),
                    Some(_) => continue,
                    None => break Ok(None),
                }
            }
        };

        let (reply, result) = reply.interruptible(lookup).await?;
        let (reply, inode) = reply.fallible(result)?;

        if let Some(inode) = inode {
            reply.found(&inode, TimeToLive::MAX)
        } else {
            reply.not_found(TimeToLive::MAX)
        }
    }

    async fn readlink<'o>(self: Farc, (_, reply, session): Op<'o, Readlink>) -> Done<'o> {
        if Inode::inode_type(&self) != Type::Symlink {
            return reply.invalid_argument();
        }

        let size = self.i_size as usize;
        if size < size_of::<[u32; 15]>() {
            return reply.target(OsStr::from_bytes(&cast_slice(&self.i_block)[..size]));
        }

        let fs = session.fs();
        let segments = async {
            /* This is unlikely to ever spill, and is guaranteed not to
             * do so for valid symlinks on any fs where block_size >= 4096.
             */
            let mut segments = SmallVec::<[&OsStr; 1]>::new();
            let (mut size, mut offset) = (size, 0);

            while size > 0 {
                let segment = fs.seek_contiguous(&self, offset)?;
                let segment = &segment[..segment.len().min(size)];

                segments.push(OsStr::from_bytes(segment));

                size -= segment.len();
                offset += segment.len() as u64;
            }

            Ok(segments)
        };

        let (reply, segments) = reply.fallible(segments.await)?;
        reply.gather_target(&segments)
    }

    async fn readdir<'o>(self: Farc, (request, reply, session): Op<'o, Readdir>) -> Done<'o> {
        let stream = session.fs().directory_stream(self, request.offset());
        reply.try_stream(stream).await?
    }
}

fn early_error<T, E: From<Errno>>(_: ()) -> Result<T, E> {
    Err(Errno::EINVAL.into())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new("ext2")
        .about("read-only ext2 FUSE driver")
        .arg(Arg::from_usage("[mount_options] -o <options>... 'See fuse(8)'").number_of_values(1))
        .arg(Arg::from_usage("<image> 'Filesystem image file'"))
        .arg(Arg::from_usage("<mountpoint> 'Filesystem mountpoint'"))
        .get_matches();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let (image, session) = {
        let (image, mountpoint) = {
            let required_path = |key| Path::new(matches.value_of(key).unwrap());
            (required_path("image"), required_path("mountpoint"))
        };

        let canonical = image.canonicalize();
        let canonical = canonical.as_ref().map(PathBuf::as_path).unwrap_or(image);

        let mut options = Options::default();
        options
            .fs_name(canonical)
            .read_only()
            .extend(matches.values_of_os("mount_options").into_iter().flatten());

        (image, mount_sync(mountpoint, &options)?)
    };

    let file = File::open(image)?;
    let backing = unsafe {
        let length = file.metadata().unwrap().len() as usize;

        let base = mmap(
            std::ptr::null_mut(),
            length,
            ProtFlags::PROT_READ,
            MapFlags::MAP_PRIVATE,
            file.as_raw_fd(),
            0,
        );

        std::slice::from_raw_parts(base.unwrap() as *const u8, length)
    };

    let superblock = if backing.len() >= 1024 + size_of::<Superblock>() {
        Some(&backing[1024..1024 + size_of::<Superblock>()])
    } else {
        None
    };

    let superblock = superblock.and_then(|superblock| try_from_bytes(superblock).ok());
    let superblock: &'static Superblock = match superblock {
        Some(superblock) => superblock,
        None => return early_error(log::error!("Bad superblock")),
    };

    if superblock.s_magic != 0xef53 {
        return early_error(log::error!("Bad magic"));
    }

    let (major, minor) = (superblock.s_rev_level, superblock.s_minor_rev_level);
    if (major, minor) != (1, 0) {
        return early_error(log::error!("Unsupported revision: {}.{}", major, minor));
    }

    let fs = Ext2 {
        backing,
        superblock,
    };

    Ok(Runtime::new()?.block_on(async { session.start(fs).await?.main_loop().await })?)
}
