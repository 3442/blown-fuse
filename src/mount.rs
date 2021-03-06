use std::{
    ffi::{OsStr, OsString},
    io,
    os::unix::{
        ffi::OsStrExt,
        io::{AsRawFd, RawFd},
        net::UnixStream,
    },
    path::{Path, PathBuf},
    process::Command,
};

use nix::{
    self, cmsg_space,
    fcntl::{fcntl, FcntlArg, FdFlag},
    sys::socket::{recvmsg, ControlMessageOwned, MsgFlags},
};

use crate::{error::MountError, session::Start, util::DumbFd};

#[derive(Default)]
pub struct Options(OsString);

impl Options {
    pub fn fs_name<O: AsRef<OsStr>>(&mut self, fs_name: O) -> &mut Self {
        self.push_key_value("fsname", fs_name)
    }

    pub fn read_only(&mut self) -> &mut Self {
        self.push("ro")
    }

    pub fn push<O: AsRef<OsStr>>(&mut self, option: O) -> &mut Self {
        self.push_parts(&[option.as_ref()])
    }

    pub fn push_key_value<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        let (key, value) = (key.as_ref(), value.as_ref());

        let assert_valid = |part: &OsStr| {
            let bytes = part.as_bytes();
            assert!(
                !bytes.is_empty() && bytes.iter().all(|b| !matches!(*b, b',' | b'=')),
                "invalid key or value: {}",
                part.to_string_lossy()
            );
        };

        assert_valid(key);
        assert_valid(value);

        self.push_parts(&[key, OsStr::new("="), value])
    }

    fn push_parts(&mut self, segment: &[&OsStr]) -> &mut Self {
        if !self.0.is_empty() {
            self.0.push(",");
        }

        let start = self.0.as_bytes().len();
        segment.iter().for_each(|part| self.0.push(part));

        let bytes = self.0.as_bytes();
        let last = bytes.len() - 1;

        assert!(
            last >= start && bytes[start] != b',' && bytes[last] != b',',
            "invalid option string: {}",
            OsStr::from_bytes(&bytes[start..]).to_string_lossy()
        );

        self
    }
}

impl<O: AsRef<OsStr>> Extend<O> for Options {
    fn extend<I: IntoIterator<Item = O>>(&mut self, iter: I) {
        iter.into_iter().for_each(|option| {
            self.push(option);
        });
    }
}

pub fn mount_sync<M>(mountpoint: M, options: &Options) -> Result<Start, MountError>
where
    M: AsRef<Path> + Into<PathBuf>,
{
    let (left_side, right_side) = UnixStream::pair()?;

    // The fusermount protocol requires us to preserve right_fd across execve()
    let right_fd = right_side.as_raw_fd();
    fcntl(
        right_fd,
        FcntlArg::F_SETFD(
            FdFlag::from_bits(fcntl(right_fd, FcntlArg::F_GETFD).unwrap()).unwrap()
                & !FdFlag::FD_CLOEXEC,
        ),
    )
    .unwrap();

    let mut command = Command::new(FUSERMOUNT_CMD);
    if !options.0.is_empty() {
        command.args(&[OsStr::new("-o"), &options.0]);
    }

    command.args(&[OsStr::new("--"), mountpoint.as_ref().as_ref()]);
    let mut fusermount = command.env("_FUSE_COMMFD", right_fd.to_string()).spawn()?;

    // recvmsg() should fail if fusermount exits (last open fd is closed)
    drop(right_side);

    let session_fd = {
        let mut buffer = cmsg_space!(RawFd);
        let message = recvmsg(
            left_side.as_raw_fd(),
            &[],
            Some(&mut buffer),
            MsgFlags::empty(),
        )
        .map_err(io::Error::from)?;

        let session_fd = match message.cmsgs().next() {
            Some(ControlMessageOwned::ScmRights(fds)) => fds.into_iter().next(),
            _ => None,
        };

        session_fd.ok_or(MountError::Fusermount)
    };

    match session_fd {
        Ok(session_fd) => Ok(Start::new(DumbFd(session_fd), mountpoint.into())),

        Err(error) => {
            drop(left_side);
            fusermount.wait()?;
            Err(error)
        }
    }
}

pub(crate) fn unmount_sync<M: AsRef<OsStr>>(mountpoint: M) -> Result<(), MountError> {
    let status = Command::new(FUSERMOUNT_CMD)
        .args(&[OsStr::new("-zuq"), OsStr::new("--"), mountpoint.as_ref()])
        .status()?;

    if status.success() {
        Ok(())
    } else {
        Err(MountError::Fusermount)
    }
}

const FUSERMOUNT_CMD: &str = "fusermount3";
