use super::{
    c_to_os,
    traits::{ReplyKnown, ReplyOk, RequestMode, RequestName},
};

use crate::{io::Mode, private_trait::Sealed, proto, Ino, Operation, Request};
use std::ffi::{CStr, OsStr};

pub enum Mkdir {}
pub enum Unlink {}
pub enum Rmdir {}
pub enum Symlink {}
pub enum Link {}

pub trait RequestTarget<'o>: Operation<'o> {
    fn target<'a>(request: &'a Request<'o, Self>) -> &'a OsStr;
}

pub trait RequestLink<'o>: Operation<'o> {
    fn source_ino(request: &Request<'o, Self>) -> Ino;
}

impl Sealed for Mkdir {}
impl Sealed for Unlink {}
impl Sealed for Rmdir {}
impl Sealed for Symlink {}
impl Sealed for Link {}

impl<'o> Operation<'o> for Mkdir {
    type RequestBody = (&'o proto::MkdirIn, &'o CStr);
    type ReplyState = ();
}

impl<'o> Operation<'o> for Unlink {
    type RequestBody = &'o CStr; // name()
    type ReplyState = ();
}

impl<'o> Operation<'o> for Rmdir {
    type RequestBody = &'o CStr; // name()
    type ReplyState = ();
}

impl<'o> Operation<'o> for Symlink {
    type RequestBody = (&'o CStr, &'o CStr); // name(), target()
    type ReplyState = ();
}

impl<'o> Operation<'o> for Link {
    type RequestBody = (&'o proto::LinkIn, &'o CStr);
    type ReplyState = ();
}

impl<'o> RequestName<'o> for Mkdir {
    fn name<'a>(request: &'a Request<'o, Self>) -> &'a OsStr {
        let (_header, name) = request.body;
        c_to_os(name)
    }
}

impl<'o> RequestMode<'o> for Mkdir {
    fn mode(request: &Request<'o, Self>) -> Mode {
        let (header, _name) = request.body;
        Mode::from_bits_truncate(header.mode)
    }
}

impl<'o> ReplyKnown<'o> for Mkdir {}

impl<'o> RequestName<'o> for Unlink {
    fn name<'a>(request: &'a Request<'o, Self>) -> &'a OsStr {
        c_to_os(request.body)
    }
}

impl<'o> ReplyOk<'o> for Unlink {}

impl<'o> RequestName<'o> for Rmdir {
    fn name<'a>(request: &'a Request<'o, Self>) -> &'a OsStr {
        c_to_os(request.body)
    }
}

impl<'o> ReplyOk<'o> for Rmdir {}

impl<'o> RequestName<'o> for Symlink {
    fn name<'a>(request: &'a Request<'o, Self>) -> &'a OsStr {
        let (name, _target) = request.body;
        c_to_os(name)
    }
}

impl<'o> RequestTarget<'o> for Symlink {
    fn target<'a>(request: &'a Request<'o, Self>) -> &'a OsStr {
        let (_name, target) = request.body;
        c_to_os(target)
    }
}

impl<'o> ReplyKnown<'o> for Symlink {}

impl<'o> RequestName<'o> for Link {
    fn name<'a>(request: &'a Request<'o, Self>) -> &'a OsStr {
        let (_header, name) = request.body;
        c_to_os(name)
    }
}

impl<'o> RequestLink<'o> for Link {
    fn source_ino(request: &Request<'o, Self>) -> Ino {
        let (header, _name) = request.body;
        Ino(header.old_ino)
    }
}

impl<'o> ReplyKnown<'o> for Link {}
