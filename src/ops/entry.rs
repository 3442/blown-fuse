use super::traits::ReplyOk;
use crate::{io::Stat, private_trait::Sealed, proto, Done, Ino, Operation, Reply, Request};

pub enum Forget {}
pub enum Getattr {}

pub trait ReplyStat<'o>: Operation<'o> {
    fn stat(reply: Reply<'o, Self>, inode: &impl Stat) -> Done<'o>;
}

impl Sealed for Forget {}
impl Sealed for Getattr {}

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

impl<'o> Request<'o, Forget> {
    pub fn forget_list(&self) -> impl '_ + Iterator<Item = (Ino, u64)> {
        use proto::OpcodeSelect::*;

        enum List<'a> {
            Single(Option<(Ino, u64)>),
            Batch(std::slice::Iter<'a, proto::ForgetOne>),
        }

        impl Iterator for List<'_> {
            type Item = (Ino, u64);

            fn next(&mut self) -> Option<Self::Item> {
                match self {
                    List::Single(single) => single.take(),
                    List::Batch(batch) => {
                        let forget = batch.next()?;
                        Some((Ino(forget.ino), forget.nlookup))
                    }
                }
            }
        }

        match self.body {
            Match((_, slice)) => List::Batch(slice.iter()),
            Alt(single) => List::Single(Some((self.ino(), single.nlookup))),
        }
    }
}

impl<'o> ReplyOk<'o> for Forget {
    fn ok(_reply: Reply<'o, Self>) -> Done<'o> {
        // No reply for forget requests
        Done::new()
    }
}

impl<'o> Request<'o, Getattr> {
    pub fn handle(&self) -> u64 {
        self.body.fh
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
