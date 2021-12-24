//! FUSE client.
//!
//! Usually, a kernel module or other OS component takes the role of the FUSE client. This module
//! is a client-wise counterpart to the rest of `blown-fuse` API. So far, this only serves the
//! purpose of having agnostic tests, but wrappers might be written in the future with it.
