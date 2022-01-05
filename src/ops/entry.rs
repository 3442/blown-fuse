use crate::{
    io::{Mode, Stat},
    private_trait::Sealed,
    proto, Done, Ino, Operation, Reply, Request,
};

use super::{
    c_to_os,
    traits::{ReplyKnown, ReplyOk, RequestHandle, RequestMode, RequestName},
};

use std::ffi::{CStr, OsStr};

pub enum Forget {}
pub enum Getattr {}
pub enum Mkdir {}

pub trait RequestForget<'o>: Operation<'o> {
    fn forget_list<'a>(request: &'a Request<'o, Self>) -> ForgetList<'a>;
}

pub trait ReplyStat<'o>: Operation<'o> {
    fn stat(reply: Reply<'o, Self>, inode: &impl Stat) -> Done<'o>;
}

pub enum ForgetList<'a> {
    Single(Option<(Ino, u64)>),
    Batch(std::slice::Iter<'a, proto::ForgetOne>),
}

impl Sealed for Forget {}
impl Sealed for Getattr {}
impl Sealed for Mkdir {}

impl<'o> Operation<'o> for Forget {
    type RequestBody = proto::OpcodeSelect<
        (&'o proto::BatchForgetIn, &'o [proto::ForgetOne]),
        &'o proto::ForgetIn,
        { proto::Opcode::BatchForget as u32 },
    >;

    type ReplyTail = ();
}

impl<'o> Operation<'o> for Getattr {
    type RequestBody = &'o proto::GetattrIn;
    type ReplyTail = ();
}

impl<'o> Operation<'o> for Mkdir {
    type RequestBody = (&'o proto::MkdirIn, &'o CStr);
    type ReplyTail = ();
}

impl<'o> RequestForget<'o> for Forget {
    fn forget_list<'a>(request: &'a Request<'o, Self>) -> ForgetList<'a> {
        use {proto::OpcodeSelect::*, ForgetList::*};

        impl Iterator for ForgetList<'_> {
            type Item = (Ino, u64);

            fn next(&mut self) -> Option<Self::Item> {
                match self {
                    Single(single) => single.take(),
                    Batch(batch) => {
                        let forget = batch.next()?;
                        Some((Ino(forget.ino), forget.nlookup))
                    }
                }
            }
        }

        match request.body {
            Match((_, slice)) => Batch(slice.iter()),
            Alt(single) => Single(Some((request.ino(), single.nlookup))),
        }
    }
}

impl<'o> ReplyOk<'o> for Forget {
    fn ok(_reply: Reply<'o, Self>) -> Done<'o> {
        // No reply for forget requests
        Done::new()
    }
}

impl<'o> RequestHandle<'o> for Getattr {
    fn handle(request: &Request<'o, Self>) -> u64 {
        request.body.fh
    }
}

impl<'o> ReplyStat<'o> for Getattr {
    fn stat(reply: Reply<'o, Self>, inode: &impl Stat) -> Done<'o> {
        let (attrs, ttl) = inode.attrs();
        let attrs = attrs.finish(inode);

        reply.single(&proto::AttrOut {
            attr_valid: ttl.seconds,
            attr_valid_nsec: ttl.nanoseconds,
            dummy: Default::default(),
            attr: attrs,
        })
    }
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
