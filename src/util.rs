use std::{
    fmt,
    os::unix::io::{IntoRawFd, RawFd},
};

use nix::unistd::{close, sysconf, SysconfVar};

pub struct DumbFd(pub RawFd);

pub struct OutputChain<'a> {
    segments: &'a [&'a [u8]],
    then: Option<&'a OutputChain<'a>>,
}

pub struct OutputChainIter<'a>(Option<&'a OutputChain<'a>>);

impl DumbFd {
    pub fn take(&mut self) -> DumbFd {
        DumbFd(std::mem::replace(&mut self.0, -1))
    }
}

impl IntoRawFd for DumbFd {
    fn into_raw_fd(self) -> RawFd {
        let fd = self.0;
        std::mem::forget(self);
        fd
    }
}

impl Drop for DumbFd {
    fn drop(&mut self) {
        if !self.0.is_negative() {
            let _ = close(self.0);
        }
    }
}

impl<'a> OutputChain<'a> {
    pub fn empty() -> Self {
        OutputChain {
            segments: &[],
            then: None,
        }
    }

    pub fn tail(segments: &'a [&'a [u8]]) -> Self {
        OutputChain {
            segments,
            then: None,
        }
    }

    pub fn preceded(&'a self, segments: &'a [&'a [u8]]) -> Self {
        OutputChain {
            segments,
            then: Some(self),
        }
    }

    pub fn iter(&self) -> OutputChainIter<'_> {
        OutputChainIter(Some(self))
    }
}

impl<'a> Iterator for OutputChainIter<'a> {
    type Item = &'a [&'a [u8]];

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.0.and_then(|chain| chain.then);
        std::mem::replace(&mut self.0, next).map(|chain| chain.segments)
    }
}

pub fn display_or<'a, T: fmt::Display + 'a>(
    maybe: Option<T>,
    default: &'a str,
) -> impl fmt::Display + 'a {
    struct Params<'a, T: fmt::Display>(Option<T>, &'a str);

    impl<T: fmt::Display> fmt::Display for Params<'_, T> {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            let Params(maybe, placeholder) = &self;
            if let Some(t) = maybe {
                write!(fmt, "{}", t)
            } else {
                fmt.write_str(placeholder)
            }
        }
    }

    Params(maybe, default)
}

pub fn page_size() -> usize {
    sysconf(SysconfVar::PAGE_SIZE).unwrap().unwrap() as usize
}
