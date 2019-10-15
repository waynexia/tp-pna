use crate::proto::kvraftpb::*;
use crate::raft::{self};

use crossbeam::channel::{unbounded as Cunbounded, Receiver as CReceiver, Sender as CSender};
use futures::sync::mpsc::unbounded;
use futures::sync::oneshot;
use futures::{Future, Stream};
use labrpc::{Error as LError, RpcFuture};
use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
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
    _dup_cmd: Arc<Mutex<HashMap<u64, String>>>,
    // Your definitions here.
    // apply_ch: Arc<UnboundedReceiver<ApplyMsg>>,
    committed_tx: CSender<ApplyResult>,
    committed_rx: CReceiver<ApplyResult>,
    _rt: Runtime,
    _token: Arc<AtomicU64>,
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
        let dup_cmd = Arc::new(Mutex::new(HashMap::new()));
        let d = dup_cmd.clone();

        let tx = committed_tx.clone();
        let apply = apply_ch
            .for_each(move |cmd| {
                // let sth_s = s.lock().unwrap();
                // drop(sth_s);
                if !cmd.command_valid {
                    return Ok(());
                }
                match labcodec::decode::<Command>(&cmd.command) {
                    Ok(command) => {
                        // lock and access HasmMap
                        let mut storage = s.lock().unwrap();
                        let mut dup_cmd = d.lock().unwrap();
                        match command.command_type {
                            // Put
                            1 => {
                                // this command is executed before
                                if dup_cmd.contains_key(&command.token) {
                                    // tx.send(ApplyResult {
                                    //     command_type: 1,
                                    //     success: true,
                                    //     err: None,
                                    //     value: None,
                                    // })
                                    // .unwrap();
                                } else {
                                    let key = command.key.clone();
                                    let value = command.value.clone().unwrap();
                                    storage.remove(&key);
                                    storage.insert(key, value);
                                    dup_cmd.insert(command.token, "".to_owned());

                                    tx.send(ApplyResult {
                                        command_type: 1,
                                        success: true,
                                        token: command.token,
                                        err: None,
                                        value: None,
                                    })
                                    .unwrap();
                                }
                            }
                            // Append
                            2 => {
                                if !dup_cmd.contains_key(&command.token) {
                                    let key = command.key.clone();
                                    let value = command.value.clone().unwrap();
                                    let prev_value =
                                        storage.get(&key).map(|s| s.to_owned()).unwrap_or_default();
                                    let new_value = format!("{}{}", prev_value, value);
                                    storage.insert(key, new_value);
                                    dup_cmd.insert(command.token, "".to_owned());

                                    tx.send(ApplyResult {
                                        command_type: 2,
                                        success: true,
                                        token: command.token,
                                        err: None,
                                        value: None,
                                    })
                                    .unwrap();
                                }
                            }
                            // Get
                            3 => {
                                if !dup_cmd.contains_key(&command.token) {
                                    let key = command.key.clone();
                                    if !storage.contains_key(&key) {
                                        dup_cmd.insert(command.token, "".to_owned());
                                        tx.send(ApplyResult {
                                            command_type: 3,
                                            success: true,
                                            token: command.token,
                                            err: Some("key does not exist".to_owned()),
                                            value: Some("".to_owned()),
                                        })
                                        .unwrap();
                                    } else {
                                        let value = storage.get(&key).unwrap().to_owned();
                                        dup_cmd.insert(command.token, value.clone());
                                        tx.send(ApplyResult {
                                            command_type: 3,
                                            success: true,
                                            token: command.token,
                                            err: None,
                                            value: Some(value),
                                        })
                                        .unwrap();
                                    }
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
            _dup_cmd: dup_cmd,
            committed_tx,
            committed_rx,
            // apply_ch,
            _rt: rt,
            _token: Arc::new(AtomicU64::new(0)),
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
        println!("start a command");
        match self.rf.start(command) {
            Ok((_, _)) => Ok(()),
            Err(_) => Err(Error::NoLeader),
        }
    }

    pub fn try_get(&self, mut args: Command) -> GetReply {
        // this lead is not a leader
        // info!("will submit a get request to {}", self.me);committed_rx
        let mut resend_cnt = 0;
        loop {
            args.resend += 1;
            if self.start(&args).is_err() || resend_cnt > 3 {
                // info!("not leader");
                return GetReply {
                    wrong_leader: true,
                    err: "not leader".to_owned(),
                    value: "".to_owned(),
                };
            }
            info!("is leader");

            if let Ok(result) = self.committed_rx.recv() {
                if result.token != args.token {
                    self.committed_tx
                        .send(result)
                        .expect("commit receiver is dropped");
                    continue;
                }
                // timeout
                // need to do re-send there?

                // do command
                // command is done in background thread.

                // return value
                info!("result: {:?}", result);
                if result.success {
                    let value = result.value.unwrap();
                    return GetReply {
                        wrong_leader: false,
                        err: "".to_owned(),
                        value,
                    };
                }
            }
            thread::sleep(Duration::from_millis(1000));
            resend_cnt += 1;
        }
    }

    pub fn try_put_append(&self, mut args: Command) -> PutAppendReply {
        let mut resend_cnt = 0;
        loop {
            args.resend += 1;
            if self.start(&args).is_err() || resend_cnt > 3 {
                // info!("not leader");
                return PutAppendReply {
                    wrong_leader: true,
                    err: "not leader".to_owned(),
                };
            }

            if let Ok(result) = self.committed_rx.recv() {
                if result.token != args.token {
                    self.committed_tx
                        .send(result)
                        .expect("commit receiver is dropped");
                    continue;
                }
                // timeout
                // need to do re-send there?

                // do command
                // command is done in background thread.

                // return value
                info!("~~~~~~~~~~~~~~~~~~~~~~~~result: {:?}", result);
                if result.success {
                    return PutAppendReply {
                        wrong_leader: false,
                        err: "".to_owned(),
                    };
                }
            }
            thread::sleep(Duration::from_millis(1000));
            resend_cnt += 1;
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
        if !self.is_leader() {
            return Box::new(futures::future::result(Ok(GetReply {
                wrong_leader: true,
                err: "not leader".to_owned(),
                value: "".to_owned(),
            })));
        }

        let mut rng = rand::thread_rng();
        let token: u64 = rng.gen();

        println!("start a get");

        let command = Command {
            command_type: 3, // Get
            key: args.key,
            value: None,
            token,
            resend: 0,
        };
        let (tx, rx) = oneshot::channel::<GetReply>();

        // let mut rng = rand::thread_rng();
        // let token: u16 = rng.gen();

        // info!("going to wait {}", token);
        let server = self.server.clone();
        thread::spawn(move || tx.send(server.try_get(command)));

        // let retval = rx.wait().unwrap_or_default();
        // info!("wait finish {}", token);
        // Box::new(futures::future::result(Ok(retval)))
        Box::new(rx.map_err(LError::Recv))
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn put_append(&self, args: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        if !self.is_leader() {
            return Box::new(futures::future::result(Ok(PutAppendReply {
                wrong_leader: true,
                err: "not leader".to_owned(),
            })));
        }

        println!("start a put / append");
        let mut rng = rand::thread_rng();
        let token: u64 = rng.gen();

        let command = Command {
            command_type: args.op,
            key: args.key,
            value: Some(args.value),
            token,
            resend: 0,
        };
        let (tx, rx) = oneshot::channel::<PutAppendReply>();

        // info!("going to wait {}", token);
        let server = self.server.clone();
        thread::spawn(move || tx.send(server.try_put_append(command)));

        // let retval = rx.wait().unwrap_or_default();
        // info!("wait finish {}", token);
        // Box::new(futures::future::result(Ok(retval)))
        Box::new(rx.map_err(LError::Recv))
    }
}
