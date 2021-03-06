/* Read-only ext2 (rev 1.0) implementation.
 *
 * This is not really async, since the whole backing storage
 * is mmap()ed for simplicity, and then treated as a static
 * slice (likely unsound, I don't care). Some yields are
 * springled in a few places in order to emulate true async
 * operations.
 *
 * Reference: <https://www.nongnu.org/ext2-doc/ext2.html>
 */

#[cfg(target_endian = "big")]
compile_error!("This example assumes a little-endian system");

use std::{
    ffi::{CStr, OsStr},
    fs::File,
    mem::size_of,
    ops::ControlFlow,
    os::unix::{ffi::OsStrExt, io::AsRawFd},
    path::{Path, PathBuf},
    time::{Duration, UNIX_EPOCH},
};

use blown_fuse::{
    io::{Attrs, Entry, EntryType, FsInfo, Gid, Ino, Known, Mode, Stat, Ttl, Uid},
    mount::{mount_sync, Options},
    ops,
    session::{Dispatch, Start},
    Done, Errno, FuseResult, Op,
};

use bytemuck::{cast_slice, from_bytes, try_from_bytes};
use bytemuck_derive::{Pod, Zeroable};
use clap::{App, Arg};
use futures_util::stream::{self, Stream, StreamExt, TryStreamExt};
use nix::sys::mman::{mmap, MapFlags, ProtFlags};
use smallvec::SmallVec;
use tokio::{self, runtime::Runtime};
use uuid::Uuid;

const EXT2_ROOT: Ino = Ino(2);

struct Ext2 {
    backing: &'static [u8],
    superblock: &'static Superblock,
}

struct Resolved {
    ino: Ino,
    inode: &'static Inode,
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
        inode: &'static Inode,
        start: u64,
    ) -> impl Stream<Item = Result<Entry<'static, Resolved>, Errno>> + '_ {
        stream::try_unfold(start, move |mut position| async move {
            loop {
                if position == inode.i_size as u64 {
                    break Ok(None); // End of stream
                }

                let bytes = self.seek_contiguous(inode, position)?;
                let (header, bytes) = bytes.split_at(size_of::<LinkedEntry>());
                let header: &LinkedEntry = from_bytes(header);

                position += header.rec_len as u64;
                if header.inode == 0 {
                    continue; // Unused entry
                }

                let ino = Ino(header.inode as u64);
                let name = OsStr::from_bytes(&bytes[..header.name_len as usize]);

                let inode = Resolved {
                    ino,
                    inode: self.inode(ino)?,
                };

                let entry = Entry {
                    inode,
                    name,
                    offset: position,
                    ttl: Ttl::MAX,
                };

                break Ok(Some((entry, position)));
            }
        })
    }

    fn inode(&self, ino: Ino) -> Result<&'static Inode, Errno> {
        let Ino(ino) = match ino {
            Ino::ROOT => EXT2_ROOT,
            EXT2_ROOT => Ino::ROOT,
            ino => ino,
        };

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

        Ok(from_bytes(&self.block(block)?[start..end]))
    }

    fn seek_contiguous(
        &self,
        inode: &'static Inode,
        position: u64,
    ) -> Result<&'static [u8], Errno> {
        let block_size = self.block_size();
        let position = position as usize;
        let (direct, offset) = (position / block_size, position % block_size);

        let out_of_bounds = || log::error!("Offset {} out of bounds", position);
        let chase = |indices: &[usize]| {
            let root: &[u8] = cast_slice(&inode.i_block);
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

impl Ext2 {
    fn init<'o>(&self, (_, reply): Op<'o, ops::Init>) -> Done<'o> {
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
            .unwrap_or_else(|| "(bad)".into());

        log::info!("UUID: {}", Uuid::from_bytes(self.superblock.s_uuid));
        log::info!("Label: {}", label.escape_debug());

        log::info!("Mounted successfully");
        reply.ok()
    }

    async fn statfs<'o>(&self, (_, reply): Op<'o, ops::Statfs>) -> Done<'o> {
        let total_blocks = self.superblock.s_blocks_count as u64;
        let free_blocks = self.superblock.s_free_blocks_count as u64;
        let available_blocks = free_blocks - self.superblock.s_r_blocks_count as u64;
        let total_inodes = self.superblock.s_inodes_count as u64;
        let free_inodes = self.superblock.s_free_inodes_count as u64;

        let info = FsInfo::default()
            .blocks(
                self.block_size() as u32,
                total_blocks,
                free_blocks,
                available_blocks,
            )
            .inodes(total_inodes, free_inodes)
            .max_filename(255);

        reply.info(&info)
    }

    async fn getattr<'o>(&self, (request, reply): Op<'o, ops::Getattr>) -> Done<'o> {
        let ino = request.ino();
        let (reply, inode) = reply.and_then(self.inode(ino))?;

        reply.stat(&Resolved { ino, inode })
    }

    async fn lookup<'o>(&self, (request, reply): Op<'o, ops::Lookup>) -> Done<'o> {
        let name = request.name();
        let (mut reply, parent) = reply.and_then(self.inode(request.ino()))?;

        //TODO: Indexed directories
        let stream = self.directory_stream(parent, 0);
        tokio::pin!(stream);

        let inode = loop {
            let (next_reply, entry) = reply.and_then(stream.try_next().await)?;
            reply = next_reply;

            match entry {
                Some(entry) if entry.name == name => break Some(entry.inode),
                Some(_) => continue,
                None => break None,
            }
        };

        if let Some(inode) = inode {
            reply.known(inode, Ttl::MAX)
        } else {
            reply.not_found_for(Ttl::MAX)
        }
    }

    async fn readlink<'o>(&self, (request, reply): Op<'o, ops::Readlink>) -> Done<'o> {
        let ino = request.ino();
        let (mut reply, inode) = reply.and_then(self.inode(ino))?;

        let resolved = Resolved { ino, inode };
        if resolved.inode_type() != EntryType::Symlink {
            return reply.invalid_argument();
        }

        let size = inode.i_size as usize;
        if size < size_of::<[u32; 15]>() {
            return reply.slice(&cast_slice(&inode.i_block)[..size]);
        }

        /* This is unlikely to ever spill, and is guaranteed not to
         * do so for valid symlinks on any fs where block_size >= 4096.
         */
        let mut segments = SmallVec::<[&[u8]; 1]>::new();
        let (mut size, mut offset) = (size, 0);

        while size > 0 {
            let (next_reply, segment) = reply.and_then(self.seek_contiguous(inode, offset))?;
            reply = next_reply;

            let segment = &segment[..segment.len().min(size)];
            segments.push(segment);

            size -= segment.len();
            offset += segment.len() as u64;
        }

        reply.gather(&segments)
    }

    async fn readdir<'o>(&self, (request, reply): Op<'o, ops::Readdir>) -> Done<'o> {
        let (reply, inode) = reply.and_then(self.inode(request.ino()))?;
        let mut reply = reply.buffered(Vec::new());

        let stream = self.directory_stream(inode, request.offset());
        tokio::pin!(stream);

        while let Some(entry) = stream.next().await {
            let (next_reply, entry) = reply.and_then(entry)?;
            let (next_reply, ()) = next_reply.entry(entry)?;
            reply = next_reply;
        }

        reply.end()
    }
}

impl Stat for Resolved {
    fn ino(&self) -> Ino {
        self.ino
    }

    fn inode_type(&self) -> EntryType {
        let inode_type = self.inode.i_mode >> 12;
        match inode_type {
            0x01 => EntryType::Fifo,
            0x02 => EntryType::CharacterDevice,
            0x04 => EntryType::Directory,
            0x06 => EntryType::BlockDevice,
            0x08 => EntryType::File,
            0x0A => EntryType::Symlink,
            0x0C => EntryType::Socket,

            _ => {
                log::error!("Inode {} has invalid type {:x}", self.ino, inode_type);
                EntryType::File
            }
        }
    }

    fn attrs(&self) -> (Attrs, Ttl) {
        let inode = self.inode;
        let (access, modify, change) = {
            let time = |seconds: u32| (UNIX_EPOCH + Duration::from_secs(seconds.into())).into();
            let (atime, mtime, ctime) = (inode.i_atime, inode.i_mtime, inode.i_ctime);

            (time(atime), time(mtime), time(ctime))
        };

        let attrs = Attrs::default()
            .size((inode.i_dir_acl as u64) << 32 | inode.i_size as u64)
            .owner(
                Uid::from_raw(inode.i_uid.into()),
                Gid::from_raw(inode.i_gid.into()),
            )
            .mode(Mode::from_bits_truncate(inode.i_mode.into()))
            .blocks(inode.i_blocks.into())
            .block_size(512)
            .times(access, modify, change)
            .links(inode.i_links_count.into());

        (attrs, Ttl::MAX)
    }
}

impl Known for Resolved {
    type Inode = Resolved;

    fn inode(&self) -> &Self::Inode {
        self
    }

    fn unveil(self) {}
}

async fn main_loop(session: Start, fs: Ext2) -> FuseResult<()> {
    let session = session.start(|op| fs.init(op)).await?;
    let mut endpoint = session.endpoint();

    loop {
        let result = endpoint.receive(|dispatch| async {
            use Dispatch::*;

            match dispatch {
                Statfs(statfs) => fs.statfs(statfs.op()?).await,
                Getattr(getattr) => fs.getattr(getattr.op()?).await,
                Lookup(lookup) => fs.lookup(lookup.op()?).await,
                Readlink(readlink) => fs.readlink(readlink.op()?).await,
                Readdir(readdir) => fs.readdir(readdir.op()?).await,

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
        None => {
            log::error!("Bad superblock");
            return Err(Errno::EINVAL.into());
        }
    };

    if superblock.s_magic != 0xef53 {
        log::error!("Bad magic");
        return Err(Errno::EINVAL.into());
    }

    let (major, minor) = (superblock.s_rev_level, superblock.s_minor_rev_level);
    if (major, minor) != (1, 0) {
        log::error!("Unsupported revision: {}.{}", major, minor);
        return Err(Errno::EINVAL.into());
    }

    let fs = Ext2 {
        backing,
        superblock,
    };

    Ok(Runtime::new()?.block_on(main_loop(session, fs))?)
}
