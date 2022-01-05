use std::{
    ffi::{CStr, OsStr},
    os::unix::ffi::OsStrExt,
};

use crate::{
    private_trait::Sealed, proto, util::OutputChain, Done, Ino, Operation, Reply, Request, Ttl,
};

use bytemuck::{bytes_of, Pod};

pub mod traits;

pub use dir::{BufferedReaddir, Lookup, Readdir};
pub use entry::{Forget, Getattr, Mkdir, Rmdir, Unlink};
pub use global::{Init, Statfs};
pub use open::{Access, Open, Opendir, Release, Releasedir};
pub use rw::{Flush, Read, Readlink, Write};
pub use xattr::{Getxattr, Listxattr, Removexattr, Setxattr};

mod dir;
mod entry;
mod global;
mod open;
mod rw;
mod xattr;

pub(crate) use global::InitState;

pub trait FromRequest<'o, O: Operation<'o>> {
    //TODO: Shouldn't be public
    fn from_request(request: &Request<'o, O>) -> Self;
}

pub enum Any {}

impl Sealed for Any {}

impl<'o> Operation<'o> for Any {
    type RequestBody = ();
    type ReplyTail = ();
}

impl<'o, O: Operation<'o>> FromRequest<'o, O> for () {
    fn from_request(_request: &Request<'o, O>) -> Self {}
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

fn c_to_os(c_str: &CStr) -> &OsStr {
    OsStr::from_bytes(c_str.to_bytes())
}
