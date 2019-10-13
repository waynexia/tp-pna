use crate::proto::kvraftpb::*;
use crate::raft::{self};

use crossbeam::channel::{unbounded as Cunbounded, Receiver as CReceiver};
use futures::sync::mpsc::unbounded;
use futures::sync::oneshot;
use futures::{Future, Stream};
use labrpc::RpcFuture;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
// use tokio;
// use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
// use tokio::prelude::*;
use tokio::runtime::Runtime;

use super::errors::*;

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    _state_machine: Arc<Mutex<HashMap<String, String>>>,
    // Your definitions here.
    // apply_ch: Arc<UnboundedReceiver<ApplyMsg>>,
    // committed_tx: CSender<Command>,
    committed_rx: CReceiver<ApplyResult>,
    _rt: Runtime,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (tx, apply_ch) = unbounded();
        // let (tx, apply_ch) = unbounded_channel();
        let (committed_tx, committed_rx) = Cunbounded();
        let raft = raft::Raft::new(servers, me, persister, tx);
        let node = raft::Node::new(raft);

        let state_machine = Arc::new(Mutex::new(HashMap::new()));
        let s = state_machine.clone();

        let apply = apply_ch
            .for_each(move |cmd| {
                let sth_s = s.lock().unwrap();
                drop(sth_s);
                if !cmd.command_valid {
                    return Ok(());
                }
                match labcodec::decode::<Command>(&cmd.command) {
                    Ok(command) => {
                        // lock and access HasmMap
                        let mut storage = s.lock().unwrap();
                        match command.command_type {
                            // Put
                            1 => {
                                let key = command.key.clone();
                                let value = command.value.clone().unwrap();
                                storage.remove(&key);
                                storage.insert(key, value);

                                committed_tx
                                    .send(ApplyResult {
                                        command_type: 1,
                                        success: true,
                                        err: None,
                                        value: None,
                                    })
                                    .unwrap();
                            }
                            // Append
                            2 => {
                                let key = command.key.clone();
                                let value = command.value.clone().unwrap();
                                let prev_value =
                                    storage.get(&key).map(|s| s.to_owned()).unwrap_or_default();
                                let new_value = format!("{}{}", prev_value, value);
                                storage.insert(key, new_value);

                                committed_tx
                                    .send(ApplyResult {
                                        command_type: 2,
                                        success: true,
                                        err: None,
                                        value: None,
                                    })
                                    .unwrap();
                            }
                            // Get
                            3 => {
                                let key = command.key.clone();
                                if !storage.contains_key(&key) {
                                    committed_tx
                                        .send(ApplyResult {
                                            command_type: 3,
                                            success: true,
                                            err: Some("key does not exist".to_owned()),
                                            value: Some("".to_owned()),
                                        })
                                        .unwrap();
                                } else {
                                    let value = storage.get(&key).unwrap().to_owned();
                                    committed_tx
                                        .send(ApplyResult {
                                            command_type: 3,
                                            success: true,
                                            err: None,
                                            value: Some(value),
                                        })
                                        .unwrap();
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                    Err(e) => {
                        debug!("decode error : {:?}", e);
                    }
                }
                Ok(())
            })
            .map_err(move |_| debug!("some error"));
        // thread::spawn(move || {
        //     let _ = apply.wait();
        // });
        let rt = Runtime::new().unwrap();
        rt.executor().spawn(apply);

        KvServer {
            rf: node,
            me,
            maxraftstate,
            _state_machine: state_machine,
            // committed_tx,
            committed_rx,
            // apply_ch,
            _rt: rt,
        }
    }

    // start raft, listen on apply_ch and apply committed command
    // need a new thread or function?
    //
    // background thread, to apply commands
    // pub fn start1(&self){
    // }

    pub fn get_state(&self) -> Arc<raft::State> {
        self.rf.get_state()
    }

    // just try to start a new command to raft
    pub fn start<M>(&self, command: &M) -> Result<()>
    where
        M: labcodec::Message,
    {
        match self.rf.start(command) {
            Ok((_, _)) => Ok(()),
            Err(e) => {
                info!("{:?}", e);
                Err(Error::NoLeader)
            }
        }
    }

    pub fn try_get(&self, args: &Command) -> GetReply {
        // this lead is not a leader
        // info!("will submit a get request to {}", self.me);
        if self.start(args).is_err() {
            // info!("not leader");
            return GetReply {
                wrong_leader: true,
                err: "not leader".to_owned(),
                value: "".to_owned(),
            };
        }
        info!("is leader");

        loop {
            if let Ok(result) = self.committed_rx.try_recv() {
                // timeout
                // need to do re-send there?

                // do command
                // command is done in background thread.

                // return value
                if result.success {
                    let value = result.value.unwrap();
                    return GetReply {
                        wrong_leader: false,
                        err: "".to_owned(),
                        value,
                    };
                }
            }
        }
    }

    pub fn try_put_append(&self, args: &Command) -> PutAppendReply {
        if self.start(args).is_err() {
            // info!("not leader");
            return PutAppendReply {
                wrong_leader: true,
                err: "not leader".to_owned(),
            };
        }

        loop {
            if let Ok(result) = self.committed_rx.try_recv() {
                // timeout
                // need to do re-send there?

                // do command
                // command is done in background thread.

                // return value
                if result.success {
                    return PutAppendReply {
                        wrong_leader: false,
                        err: "not leader".to_owned(),
                    };
                }
            }
        }
    }
}

impl KvServer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = &self.me;
        let _ = &self.maxraftstate;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    server: Arc<KvServer>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        // Your code here.
        // crate::your_code_here(kv);
        Node {
            server: Arc::new(kv),
        }
    }

    /// the tester calls Kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in Kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // Your code here, if desired.
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> Arc<raft::State> {
        // Your code here.
        // raft::State {
        //     ..Default::default()
        // }
        self.server.get_state()
    }
}

impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn get(&self, args: GetRequest) -> RpcFuture<GetReply> {
        let command = Command {
            command_type: 3, // Get
            key: args.key,
            value: None,
        };
        let (tx, rx) = oneshot::channel::<GetReply>();

        let server = self.server.clone();
        thread::spawn(move || tx.send(server.try_get(&command)));

        let retval = rx.wait().unwrap_or_default();
        Box::new(futures::future::result(Ok(retval)))
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn put_append(&self, args: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        let command = Command {
            command_type: args.op,
            key: args.key,
            value: Some(args.value),
        };
        let (tx, rx) = oneshot::channel::<PutAppendReply>();

        let server = self.server.clone();
        thread::spawn(move || tx.send(server.try_put_append(&command)));

        let retval = rx.wait().unwrap_or_default();
        Box::new(futures::future::result(Ok(retval)))
    }
}
