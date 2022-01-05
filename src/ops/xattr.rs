use crate::{
    private_trait::Sealed, proto, util::OutputChain, Done, Errno, Operation, Reply, Request,
};

use super::{
    c_to_os,
    traits::{ReplyGather, ReplyNotFound, ReplyOk, RequestData, RequestName, RequestSize},
};

use std::ffi::{CStr, OsStr};

pub enum Setxattr {}
pub enum Getxattr {}
pub enum Listxattr {}
pub enum Removexattr {}

pub struct XattrReadState {
    size: u32,
}

pub trait ReplyXattrRead<'o>: Operation<'o> {
    fn requires_size(reply: Reply<'o, Self>, size: u32) -> Done<'o>;

    fn buffer_too_small(reply: Reply<'o, Self>) -> Done<'o> {
        reply.fail(Errno::ERANGE)
    }
}

impl Sealed for Setxattr {}
impl Sealed for Getxattr {}
impl Sealed for Listxattr {}
impl Sealed for Removexattr {}

impl<'o> Operation<'o> for Setxattr {
    // header, name, value
    type RequestBody = (&'o proto::SetxattrIn, &'o CStr, &'o [u8]);
    type ReplyState = ();
}

impl<'o> Operation<'o> for Getxattr {
    type RequestBody = (&'o proto::GetxattrIn, &'o CStr);
    type ReplyState = XattrReadState;
}

impl<'o> Operation<'o> for Listxattr {
    type RequestBody = &'o proto::ListxattrIn;
    type ReplyState = XattrReadState;
}

impl<'o> Operation<'o> for Removexattr {
    type RequestBody = &'o CStr;
    type ReplyState = ();
}

impl<'o> RequestName<'o> for Setxattr {
    fn name<'a>(request: &'a Request<'o, Self>) -> &'a OsStr {
        let (_header, name, _value) = request.body;
        c_to_os(name)
    }
}

//TODO: flags
impl<'o> RequestData<'o> for Setxattr {
    fn data<'a>(request: &'a Request<'o, Self>) -> &'a [u8] {
        let (_header, _name, value) = request.body;
        value
    }
}

impl<'o> ReplyOk<'o> for Setxattr {}

impl<'o> ReplyNotFound<'o> for Setxattr {
    fn not_found(reply: Reply<'o, Self>) -> Done<'o> {
        reply.fail(Errno::ENODATA)
    }
}

impl<'o> RequestSize<'o> for Getxattr {
    fn size(request: &Request<'o, Self>) -> u32 {
        request.body.0.size
    }
}

impl<'o> RequestName<'o> for Getxattr {
    fn name<'a>(request: &'a Request<'o, Self>) -> &'a OsStr {
        c_to_os(request.body.1)
    }
}

impl<'o> ReplyNotFound<'o> for Getxattr {
    fn not_found(reply: Reply<'o, Self>) -> Done<'o> {
        reply.fail(Errno::ENODATA)
    }
}

impl<'o> ReplyGather<'o> for Getxattr {
    fn gather(reply: Reply<'o, Self>, fragments: &[&[u8]]) -> Done<'o> {
        let size = fragments
            .iter()
            .map(|fragment| fragment.len())
            .sum::<usize>()
            .try_into()
            .expect("Extremely large xattr");

        if reply.state.size == 0 {
            return reply.requires_size(size);
        } else if reply.state.size < size {
            return reply.buffer_too_small();
        }

        reply.chain(OutputChain::tail(fragments))
    }
}

impl<'o> ReplyXattrRead<'o> for Getxattr {
    fn requires_size(reply: Reply<'o, Self>, size: u32) -> Done<'o> {
        assert_eq!(reply.state.size, 0);

        reply.single(&proto::GetxattrOut {
            size,
            padding: Default::default(),
        })
    }
}

impl<'o> RequestSize<'o> for Listxattr {
    fn size(request: &Request<'o, Self>) -> u32 {
        request.body.getxattr_in.size
    }
}

impl<'o> ReplyXattrRead<'o> for Listxattr {
    //TODO: buffered(), gather()

    fn requires_size(reply: Reply<'o, Self>, size: u32) -> Done<'o> {
        assert_eq!(reply.state.size, 0);

        reply.single(&proto::ListxattrOut {
            getxattr_out: proto::GetxattrOut {
                size,
                padding: Default::default(),
            },
        })
    }
}

impl<'o> RequestName<'o> for Removexattr {
    fn name<'a>(request: &'a Request<'o, Self>) -> &'a OsStr {
        c_to_os(request.body)
    }
}

impl<'o> ReplyOk<'o> for Removexattr {}

impl<'o> ReplyNotFound<'o> for Removexattr {
    fn not_found(reply: Reply<'o, Self>) -> Done<'o> {
        reply.fail(Errno::ENODATA)
    }
}
