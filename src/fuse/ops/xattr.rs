use std::ffi::{CStr, OsStr};
use crate::{proto, util::OutputChain, Errno};
use crate::fuse::{private_trait::Sealed, Done, Operation, Reply, Request};
use super::c_to_os;

pub enum Setxattr {}
pub enum Getxattr {}
pub enum Listxattr {}
pub enum Removexattr {}

pub struct XattrReadState {
    size: u32,
}

impl Sealed for Setxattr {}
impl Sealed for Getxattr {}
impl Sealed for Listxattr {}
impl Sealed for Removexattr {}

impl<'o> Operation<'o> for Setxattr {
    // header, name, value
    type RequestBody = (&'o proto::SetxattrIn, &'o CStr, &'o [u8]);
    type ReplyTail = ();
}

impl<'o> Operation<'o> for Getxattr {
    type RequestBody = (&'o proto::GetxattrIn, &'o CStr);
    type ReplyTail = XattrReadState;
}

impl<'o> Operation<'o> for Listxattr {
    type RequestBody = &'o proto::ListxattrIn;
    type ReplyTail = XattrReadState;
}

impl<'o> Operation<'o> for Removexattr {
    type RequestBody = &'o CStr;
    type ReplyTail = ();
}

//TODO: flags
impl<'o> Request<'o, Setxattr> {
    pub fn name(&self) -> &OsStr {
        let (_header, name, _value) = self.body;
        c_to_os(name)
    }

    pub fn value(&self) -> &[u8] {
        let (_header, _name, value) = self.body;
        value
    }
}

impl<'o> Reply<'o, Setxattr> {
    pub fn ok(self) -> Done<'o> {
        self.empty()
    }

    pub fn not_found(self) -> Done<'o> {
        self.fail(Errno::ENODATA)
    }
}

impl<'o> Request<'o, Getxattr> {
    pub fn size(&self) -> u32 {
        self.body.0.size
    }

    pub fn name(&self) -> &OsStr {
        c_to_os(self.body.1)
    }
}

impl<'o> Reply<'o, Getxattr> {
    pub fn slice(self, value: &[u8]) -> Done<'o> {
        let size = value.len().try_into().expect("Extremely large xattr");
        if self.tail.size == 0 {
            return self.value_size(size);
        } else if self.tail.size < size {
            return self.buffer_too_small();
        }

        self.chain(OutputChain::tail(&[value]))
    }

    pub fn value_size(self, size: u32) -> Done<'o> {
        assert_eq!(self.tail.size, 0);

        self.single(&proto::GetxattrOut {
            size,
            padding: Default::default(),
        })
    }

    pub fn buffer_too_small(self) -> Done<'o> {
        self.fail(Errno::ERANGE)
    }

    pub fn not_found(self) -> Done<'o> {
        self.fail(Errno::ENODATA)
    }
}

impl<'o> Request<'o, Listxattr> {
    pub fn size(&self) -> u32 {
        self.body.getxattr_in.size
    }
}

impl<'o> Reply<'o, Listxattr> {
    //TODO: buffered(), gather()

    pub fn value_size(self, size: u32) -> Done<'o> {
        assert_eq!(self.tail.size, 0);

        self.single(&proto::ListxattrOut {
            getxattr_out: proto::GetxattrOut {
                size,
                padding: Default::default(),
            },
        })
    }

    pub fn buffer_too_small(self) -> Done<'o> {
        self.fail(Errno::ERANGE)
    }
}

impl<'o> Request<'o, Removexattr> {
    pub fn name(&self) -> &OsStr {
        c_to_os(self.body)
    }
}

impl<'o> Reply<'o, Removexattr> {
    pub fn ok(self) -> Done<'o> {
        self.empty()
    }
}
