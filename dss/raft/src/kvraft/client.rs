use rand::Rng;
use std::fmt;
use std::sync::mpsc::{channel, Receiver};

use crate::proto::kvraftpb::*;
use futures::Future;

use super::errors::*;
// use labrpc::RpcFuture;

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    pub num_server: usize,
    // You will have to modify this struct.
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        let num_server = servers.len();
        Clerk {
            name,
            servers,
            num_server,
        }
        // crate::your_code_here((name, servers))
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].get(args).unwrap();
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        // crate::your_code_here(key)

        let mut rng = rand::thread_rng();

        // random choose a server to send command
        let server_index = rng.gen_range(0, self.num_server);
        let request = GetRequest { key };
        let rx = self.send_get_rpc(server_index, &request);

        while let Ok(result) = rx.try_recv() {
            if let Ok(get_reply) = result {
                return get_reply.value;
            }
        }

        "".to_owned()
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        // You will have to modify this function.

        let mut rng = rand::thread_rng();
        let server_index = rng.gen_range(0, self.num_server);
        let (key, value, op) = match op {
            Op::Put(key, value) => (key, value, 1),
            Op::Append(key, value) => (key, value, 2),
        };
        let request = PutAppendRequest { key, value, op };

        let rx = self.send_put_append_rpc(server_index, &request);

        while let Ok(result) = rx.try_recv() {
            if let Ok(_get_reply) = result {
                // return get_reply.value;
            }
        }

        // "".to_owned()
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}

// private util functions
impl Clerk {
    fn send_get_rpc(&self, server_index: usize, args: &GetRequest) -> Receiver<Result<GetReply>> {
        let server = &self.servers[server_index];
        let (tx, rx) = channel();

        server.spawn(server.get(args).map_err(Error::Rpc).then(move |res| {
            tx.send(res).unwrap_or_default();
            Ok(())
        }));

        rx
    }

    fn send_put_append_rpc(
        &self,
        server_index: usize,
        args: &PutAppendRequest,
    ) -> Receiver<Result<PutAppendReply>> {
        let server = &self.servers[server_index];
        let (tx, rx) = channel();

        server.spawn(
            server
                .put_append(args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    tx.send(res).unwrap_or_default();
                    Ok(())
                }),
        );

        rx
    }
}
