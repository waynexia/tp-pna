use crate::proto::kvraftpb::*;
use crate::raft::{self};

// use crossbeam::channel::{unbounded as Cunbounded, Receiver as CReceiver, Sender as CSender};
use futures::sync::mpsc::unbounded;
use futures::sync::oneshot;
use futures::sync::oneshot::Sender as OSender;
use futures::{Future, Stream};
use labrpc::{Error as LError, RpcFuture};
use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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
    dup_cmd: Arc<Mutex<HashMap<u64, OSender<ApplyResult>>>>,
    // Your definitions here.
    // apply_ch: Arc<UnboundedReceiver<ApplyMsg>>,
    // committed_tx: CSender<ApplyResult>,
    // committed_rx: CReceiver<ApplyResult>,
    _rt: Runtime,
    // token for received command
    rcv_token: Arc<AtomicU64>,
    // token for applied command
    _apl_token: Arc<AtomicU64>,
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
        // let (committed_tx, committed_rx) = Cunbounded();
        let raft = raft::Raft::new(servers, me, persister, tx.clone());
        let node = raft::Node::new(raft);

        let state_machine = Arc::new(Mutex::new(HashMap::new()));
        let s = state_machine.clone();
        let dup_cmd: Arc<Mutex<HashMap<u64, OSender<ApplyResult>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let apl_token = Arc::new(AtomicU64::new(0));
        let d = dup_cmd.clone();

        // let tx = committed_tx.clone();
        let re_apply_tx = tx.clone();
        let apl_token_for_capture = apl_token.clone();
        // let me_ = me.clone();
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
                        let apply_token = apl_token_for_capture.load(Ordering::SeqCst);
                        // a legal command but need to be applied later
                        if command.token > apply_token + 1 {
                            debug!(
                                "me :{}, skiped apply token {}, send into re-apply",
                                me, apply_token
                            );
                            re_apply_tx
                                .unbounded_send(cmd)
                                .expect("failed to re-send apply message");
                            return Ok(());
                        } else {
                            debug!("me :{}, apply token {}", me, apply_token);
                            apl_token_for_capture.store(
                                apl_token_for_capture.load(Ordering::SeqCst) + 1,
                                Ordering::SeqCst,
                            );
                        }

                        let mut storage = s.lock().unwrap();
                        let mut dup_cmd = d.lock().unwrap();
                        match command.command_type {
                            // Put
                            1 => {
                                // this command is executed before
                                if dup_cmd.contains_key(&command.token) {
                                    let key = command.key.clone();
                                    let value = command.value.clone().unwrap();
                                    let tx = dup_cmd.remove(&command.token).unwrap();
                                    storage.remove(&key);
                                    storage.insert(key, value);
                                    // dup_cmd.insert(command.token, "".to_owned());

                                    tx.send(ApplyResult {
                                        command_type: 1,
                                        success: true,
                                        wrong_leader: false,
                                        err: None,
                                        value: None,
                                    })
                                    .unwrap();
                                }
                            }
                            // Append
                            2 => {
                                if dup_cmd.contains_key(&command.token) {
                                    let key = command.key.clone();
                                    let value = command.value.clone().unwrap();
                                    let prev_value =
                                        storage.get(&key).map(|s| s.to_owned()).unwrap_or_default();
                                    let new_value = format!("{}{}", prev_value, value);
                                    storage.insert(key, new_value);
                                    let tx = dup_cmd.remove(&command.token).unwrap();
                                    // dup_cmd.insert(command.token, "".to_owned());

                                    tx.send(ApplyResult {
                                        command_type: 2,
                                        success: true,
                                        wrong_leader: false,
                                        err: None,
                                        value: None,
                                    })
                                    .unwrap();
                                }
                            }
                            // Get
                            3 => {
                                if dup_cmd.contains_key(&command.token) {
                                    let key = command.key.clone();
                                    let tx = dup_cmd.remove(&command.token).unwrap();
                                    if !storage.contains_key(&key) {
                                        // dup_cmd.insert(command.token, "".to_owned());
                                        tx.send(ApplyResult {
                                            command_type: 3,
                                            success: true,
                                            wrong_leader: false,
                                            err: Some("key does not exist".to_owned()),
                                            value: Some("".to_owned()),
                                        })
                                        .unwrap();
                                    } else {
                                        let value = storage.get(&key).unwrap().to_owned();
                                        // dup_cmd.insert(command.token, value.clone());
                                        tx.send(ApplyResult {
                                            command_type: 3,
                                            success: true,
                                            wrong_leader: false,
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
            dup_cmd,
            // committed_tx,
            // committed_rx,
            // apply_ch,
            _rt: rt,
            rcv_token: Arc::new(AtomicU64::new(0)),
            _apl_token: apl_token,
        }
    }

    pub fn get_state(&self) -> Arc<raft::State> {
        self.rf.get_state()
    }

    fn get_rcv_token_and_increase(&self) -> u64 {
        let retval = self.rcv_token.load(Ordering::SeqCst);
        self.rcv_token.store(retval + 1, Ordering::SeqCst);
        retval
    }

    // just try to start a new command to raft
    pub fn start<M>(&self, command: &M) -> Result<()>
    where
        M: labcodec::Message,
    {
        info!("start a command");
        match self.rf.start(command) {
            Ok((_, _)) => Ok(()),
            Err(_) => Err(Error::NoLeader),
        }
    }

    pub fn try_get(&self, mut args: Command, tx: OSender<ApplyResult>) {
        let token = self.get_rcv_token_and_increase();
        args.token = token;
        let mut dup_cmd = self.dup_cmd.lock().unwrap();
        dup_cmd.insert(token, tx);
        drop(dup_cmd);
        for resend_cnt in 0..3 {
            if self.start(&args).is_err() || resend_cnt > 3 {
                // info!("not leader");
                let mut dup_cmd = self.dup_cmd.lock().unwrap();
                let tx = dup_cmd.remove(&token).unwrap();
                let _ = tx.send(ApplyResult {
                    command_type: args.command_type,
                    wrong_leader: true,
                    success: false,
                    err: Some("failed to reach consensuce".to_owned()),
                    value: Some("".to_owned()),
                });
                return;
            }
            thread::sleep(Duration::from_millis(1000));

            // check if applied
            let dup_cmd = self.dup_cmd.lock().unwrap();
            if !dup_cmd.contains_key(&token) {
                return;
            }
            drop(dup_cmd);

            // resend_cnt += 1;
        }
    }

    pub fn try_put_append(&self, mut args: Command, tx: OSender<ApplyResult>) {
        let token = self.get_rcv_token_and_increase();
        args.token = token;
        let mut dup_cmd = self.dup_cmd.lock().unwrap();
        dup_cmd.insert(token, tx);
        drop(dup_cmd);
        for resend_cnt in 0..3 {
            if self.start(&args).is_err() || resend_cnt > 3 {
                // info!("not leader");
                let mut dup_cmd = self.dup_cmd.lock().unwrap();
                let tx = dup_cmd.remove(&token).unwrap();
                let _ = tx.send(ApplyResult {
                    command_type: args.command_type,
                    wrong_leader: true,
                    success: false,
                    err: Some("failed to reach consensuce".to_owned()),
                    value: Some("".to_owned()),
                });
                return;
            }
            thread::sleep(Duration::from_millis(1000));

            // check if applied
            let dup_cmd = self.dup_cmd.lock().unwrap();
            if !dup_cmd.contains_key(&token) {
                return;
            }
            drop(dup_cmd);
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
        self.server.get_state()
    }
}

impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn get(&self, args: GetRequest) -> RpcFuture<ApplyResult> {
        if !self.is_leader() {
            return Box::new(futures::future::result(Ok(ApplyResult {
                command_type: 3, //Get
                wrong_leader: true,
                success: false,
                err: Some("not leader".to_owned()),
                value: Some("".to_owned()),
            })));
        }
        info!("start a get");

        let command = Command {
            command_type: 3, // Get
            key: args.key,
            value: None,
            token: 0,
        };
        let (tx, rx) = oneshot::channel::<ApplyResult>();
        self.server.try_get(command, tx);
        Box::new(rx.map_err(LError::Recv))
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn put_append(&self, args: PutAppendRequest) -> RpcFuture<ApplyResult> {
        if !self.is_leader() {
            return Box::new(futures::future::result(Ok(ApplyResult {
                command_type: args.op,
                wrong_leader: true,
                success: false,
                err: Some("not leader".to_owned()),
                value: Some("".to_owned()),
            })));
        }

        info!("start a put / append");
        let mut rng = rand::thread_rng();
        let token: u64 = rng.gen();

        let command = Command {
            command_type: args.op,
            key: args.key,
            value: Some(args.value),
            token,
        };
        let (tx, rx) = oneshot::channel::<ApplyResult>();
        self.server.try_put_append(command, tx);
        Box::new(rx.map_err(LError::Recv))
    }
}
