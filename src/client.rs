//! FUSE client.
//!
//! Usually, a kernel module or other OS component takes the role of the FUSE client. This module
//! is a client-wise counterpart to the rest of `blown-fuse` API. So far, this only serves the
//! purpose of having agnostic tests, but wrappers might be written in the future with it.

/*use crate::{proto, FuseResult};

#[cfg(feature = "server")]
use crate::session;

struct Client {}

struct RequestContext<'a> {
    client: &'a,
    uid: Uid,
    gid: Gid,
    pid: Pid,
}

impl Client {
    pub fn context(&self, uid: Uid, gid: Gid, pid: Pid) -> RequestContext<'_> {
        RequestContext {
            client: &self,
            uid,
            gid,
            pid,
        }
    }
}

impl RequestContext<'_> {
    pub async fn lookup(&self) -> FuseResult<> {
        self.tail()
    }
}

struct Start {}

impl Start {
    pub async fn start(self) -> FuseResult<Session> {

    }
}

#[cfg(feature = "server")]
pub fn channel() -> std::io::Result<(session::Start, self::Start)> {
    let client_start = ;
    let server_start = ;

    (client_stasrt, server_start)
}*/
