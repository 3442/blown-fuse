use super::traits::{ReplyOk, RequestHandle};
use crate::{io::Stat, private_trait::Sealed, proto, Done, Ino, Operation, Reply, Request};

pub enum Forget {}
pub enum Getattr {}
pub enum Bmap {}

pub trait RequestForget<'o>: Operation<'o> {
    fn forget_list<'a>(request: &'a Request<'o, Self>) -> ForgetList<'a>;
}

pub trait RequestBlock<'o>: Operation<'o> {
    fn block(request: &Request<'o, Self>) -> u64;
    fn block_size(request: &Request<'o, Self>) -> u32;
}

pub trait ReplyStat<'o>: Operation<'o> {
    fn stat(reply: Reply<'o, Self>, inode: &impl Stat) -> Done<'o>;
}

pub trait ReplyBlock<'o>: Operation<'o> {
    fn block(reply: Reply<'o, Self>, block: u64) -> Done<'o>;
}

pub enum ForgetList<'a> {
    Single(Option<(Ino, u64)>),
    Batch(std::slice::Iter<'a, proto::ForgetOne>),
}

impl Sealed for Forget {}
impl Sealed for Getattr {}
impl Sealed for Bmap {}

impl<'o> Operation<'o> for Forget {
    type RequestBody = proto::OpcodeSelect<
        (&'o proto::BatchForgetIn, &'o [proto::ForgetOne]),
        &'o proto::ForgetIn,
        { proto::Opcode::BatchForget as u32 },
    >;

    type ReplyState = ();
}

impl<'o> Operation<'o> for Getattr {
    type RequestBody = &'o proto::GetattrIn;
    type ReplyState = ();
}

impl<'o> Operation<'o> for Bmap {
    type RequestBody = &'o proto::BmapIn;
    type ReplyState = ();
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

impl<'o> RequestBlock<'o> for Bmap {
    fn block(request: &Request<'o, Self>) -> u64 {
        request.body.block
    }

    fn block_size(request: &Request<'o, Self>) -> u32 {
        request.body.block_size
    }
}

impl<'o> ReplyBlock<'o> for Bmap {
    fn block(reply: Reply<'o, Self>, block: u64) -> Done<'o> {
        reply.single(&proto::BmapOut { block })
    }
}
