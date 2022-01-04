use std::{
    ffi::{CStr, OsStr},
    os::unix::ffi::OsStrExt,
};

use crate::{private_trait::Sealed, util::OutputChain, Done, Operation, Reply, Request};
use bytemuck::{bytes_of, Pod};

mod dir;
mod entry;
mod global;
mod open;
mod rw;
mod xattr;

pub use dir::{BufferedReaddir, Lookup, Readdir};
pub use entry::{Forget, Getattr};
pub use global::{Init, Statfs};
pub use open::{Access, Open, Opendir, Release, Releasedir};
pub use rw::{Flush, Read, Readlink, Write};
pub use xattr::{Getxattr, Listxattr, Removexattr, Setxattr};

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

fn c_to_os(c_str: &CStr) -> &OsStr {
    OsStr::from_bytes(c_str.to_bytes())
}
