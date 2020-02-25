use crate::proto::kvraftpb::*;
use crate::raft::{self};

// use crossbeam::channel::{unbounded as Cunbounded, Receiver as CReceiver, Sender as CSender};
use futures::sync::mpsc::unbounded;
use futures::sync::oneshot;
// use futures::sync::oneshot::Sender as OSender;
use futures::{Future, Stream};
use labrpc::{Error as LError, RpcFuture};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
// use tokio;
// use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
// use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio02::runtime::Runtime as tokio02_Runtime;
use tokio02::sync::oneshot::{
    channel as tokio_oneshot, Receiver as TOReceiver, Sender as TOSender,
};

use super::errors::*;

use crate::raft::ApplyMsg;

const MAX_RESEND_COUNT: u64 = 10;
const TIMEOUT_INTERVAL: u64 = 1000; // in ms

type ReplyBuffer = HashMap<(u64, u64), TOSender<ApplyResult>>;
type CommandBuffer = HashMap<(u64, u64), Command>;

pub struct KvServer {
    pub rf: raft::Node,
    // peer number
    me: usize,
    // snapshot indicator
    maxraftstate: Option<usize>,

    _storage: Arc<Mutex<HashMap<String, String>>>,
    // buffer reply channel. <(term, exec_index), tx>
    pub reply_buffer: Arc<Mutex<ReplyBuffer>>,
    // buffer unordered commands
    pub command_buffer: Arc<Mutex<CommandBuffer>>,
    recv_term: Arc<AtomicU64>,
    pub exec_term: Arc<AtomicU64>,
    // the number of last executed command
    pub exec_idx: Arc<AtomicU64>,
    // received from client
    pub recv_idx: Arc<AtomicU64>,
    _rt: Runtime,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        let (tx, apply_ch) = unbounded();
        let raft = raft::Raft::new(servers, me, persister, tx);
        let node = raft::Node::new(raft);

        let storage = Arc::new(Mutex::new(HashMap::new()));
        let reply_buffer = Arc::new(Mutex::new(HashMap::new()));
        let command_buffer = Arc::new(Mutex::new(HashMap::new()));
        let exec_idx = Arc::new(AtomicU64::new(0));
        let exec_term = Arc::new(AtomicU64::new(0));

        // let re_apply_tx = tx;
        let exec_term_tomove = exec_term.clone();
        let exec_idx_tomove = exec_idx.clone();
        let storage_tomove = storage.clone();
        let reply_buffer_tomove = reply_buffer.clone();
        let command_buffer_tomove = command_buffer.clone();
        let apply = apply_ch.for_each(move |cmd| {
            let exec_term = exec_term_tomove.clone();
            let exec_idx = exec_idx_tomove.clone();
            let storage = storage_tomove.clone();
            let reply_buffer = reply_buffer_tomove.clone();
            let command_buffer = command_buffer_tomove.clone();
            KvServer::execute_command(
                exec_term,
                cmd,
                exec_idx,
                storage,
                reply_buffer,
                command_buffer,
            )
            .unwrap_or_default();
            Ok(())
        });

        let rt = Runtime::new().unwrap();
        rt.executor().spawn(apply);

        KvServer {
            rf: node,
            me,
            maxraftstate,
            _storage: storage,
            reply_buffer,
            command_buffer,
            exec_term,
            exec_idx,
            recv_term: Arc::new(AtomicU64::new(0)),
            recv_idx: Arc::new(AtomicU64::new(0)),
            _rt: rt,
        }
    }

    pub fn execute_command(
        exec_term: Arc<AtomicU64>,
        cmd: ApplyMsg,
        exec_idx: Arc<AtomicU64>,
        storage: Arc<Mutex<HashMap<String, String>>>,
        reply_buffer_lock: Arc<Mutex<ReplyBuffer>>,
        command_buffer_lock: Arc<Mutex<CommandBuffer>>,
    ) -> Result<()> {
        if !cmd.command_valid {
            return Ok(());
        }
        let mut should_continue = true;
        match labcodec::decode::<Command>(&cmd.command) {
            Ok(mut command) => {
                while should_continue {
                    info!("going to execute: {:?}", command);
                    should_continue = false;
                    match KvServer::comp_header(
                        (command.term, command.token),
                        &exec_term,
                        &exec_idx,
                    ) {
                        // match command.token.cmp(&exec_idx.load(Ordering::SeqCst)) {
                        std::cmp::Ordering::Less => {
                            // executed, ignore
                            return Ok(());
                        }
                        std::cmp::Ordering::Equal => {
                            // execute, reply, check next index in buffer
                            exec_idx.fetch_add(1, Ordering::SeqCst);
                            let mut err = None;
                            let mut value = None;
                            // get reply channel
                            let mut reply_buffer = reply_buffer_lock.lock().unwrap();
                            // follower server need not to report
                            let reply_ch = reply_buffer.remove(&(command.term, command.token));
                            drop(reply_buffer);
                            // execute command
                            let mut storage = storage.lock().unwrap();
                            match command.command_type {
                                // Put
                                1 => {
                                    storage.remove(&command.key);
                                    storage.insert(
                                        command.key.clone(),
                                        command.value.clone().unwrap(),
                                    );
                                }
                                // Append
                                2 => {
                                    let prev_value = storage
                                        .get(&command.key)
                                        .map(|s| s.to_owned())
                                        .unwrap_or_default();
                                    let new_value =
                                        format!("{}{}", prev_value, command.value.clone().unwrap());
                                    storage.insert(command.key.clone(), new_value);
                                }
                                // Get
                                3 => {
                                    if !storage.contains_key(&command.key) {
                                        err = Some("key does not exist".to_owned());
                                        value = Some("".to_owned());
                                    } else {
                                        value = Some(storage.get(&command.key).unwrap().to_owned());
                                    }
                                }
                                _ => unreachable!(),
                            }
                            if let Some(channel) = reply_ch {
                                channel
                                    .send(ApplyResult {
                                        command_type: command.command_type,
                                        success: true,
                                        wrong_leader: false,
                                        err,
                                        value,
                                    })
                                    .unwrap_or_default();
                            }
                            // consider next command in buffer
                            let mut command_buffer = command_buffer_lock.lock().unwrap();
                            should_continue = command_buffer.contains_key(&(
                                exec_term.load(Ordering::SeqCst),
                                exec_idx.load(Ordering::SeqCst),
                            ));
                            if should_continue {
                                command = command_buffer
                                    .remove(&(
                                        exec_term.load(Ordering::SeqCst),
                                        exec_idx.load(Ordering::SeqCst),
                                    ))
                                    .unwrap();
                            }
                        }
                        std::cmp::Ordering::Greater => {
                            // out of order, store command into buffer
                            let mut command_buffer = command_buffer_lock.lock().unwrap();
                            command_buffer.insert((command.term, command.token), command.clone());
                            drop(command_buffer);
                        }
                    };
                }
            }
            Err(e) => {
                info!("decode error: {:?}", e);
            }
        }
        Ok(())
    }

    /// Header consist of raft's term and command's index in this term.
    /// It is used to identify commands' ordering and duplication.
    /// Index starts from 0 and goes monotone increase in a single term.
    fn allc_header(&self) -> (u64, u64) {
        let term = self.get_term();
        let curr_term = self.recv_term.load(Ordering::SeqCst);

        // consider data race
        if term != curr_term {
            let recv_index = self.recv_idx.clone();
            self.recv_term
                .fetch_update(
                    move |_| {
                        recv_index.store(0, Ordering::SeqCst);
                        Some(term)
                    },
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .unwrap();
        }

        (term, self.recv_idx.fetch_add(1, Ordering::SeqCst))
    }

    pub fn comp_header(
        header: (u64, u64),
        exec_term: &Arc<AtomicU64>,
        exec_index: &Arc<AtomicU64>,
    ) -> std::cmp::Ordering {
        let (term, index) = header;
        if term != exec_term.load(Ordering::SeqCst) {
            // term has changed. which means:
            // 1), all commands in previous term is received by server. Thus two buffer
            // should be empty.
            // 2), need to reset exec_idx
            exec_term.store(term, Ordering::SeqCst);
            exec_index.store(0, Ordering::SeqCst);
        }
        index.cmp(&exec_index.load(Ordering::SeqCst))
    }

    pub fn get_state(&self) -> Arc<raft::State> {
        self.rf.get_state()
    }

    pub fn get_term(&self) -> u64 {
        self.rf.term()
    }

    pub async fn start_command(&self, mut args: Command) -> ApplyResult {
        if !self.get_state().is_leader() {
            return ApplyResult {
                command_type: args.command_type,
                wrong_leader: true,
                success: false,
                err: Some("not leader".to_owned()),
                value: None,
            };
        }
        // set header
        let (term, idx) = self.allc_header();
        args.term = term;
        args.token = idx;

        // for server to report execute result
        let (result_tx, result_rx) = tokio_oneshot();
        let mut reply_buffer = self.reply_buffer.lock().unwrap();
        if reply_buffer.contains_key(&(term, idx)) {
            // todo: change error type
            // return Err(Error::Others);
            info!("{}, {}", term, idx);
            unreachable!();
        }
        reply_buffer.insert((term, idx), result_tx);
        drop(reply_buffer);

        match self.start(&args, result_rx).await {
            Ok(result) => result,
            Err(e) => ApplyResult {
                command_type: args.command_type,
                wrong_leader: e == Error::NoLeader,
                success: false,
                err: Some(format!("{:?}", e)),
                value: None,
            },
        }
    }

    pub async fn start<M>(
        &self,
        command: &M,
        // exec_index: u64,
        mut result_rx: TOReceiver<ApplyResult>,
    ) -> Result<ApplyResult>
    where
        M: labcodec::Message,
    {
        info!("start a command");

        let mut timeout_cnt = 0;
        let mut delay = tokio02::time::delay_for(Duration::from_millis(0));

        loop {
            tokio02::select! {
                _ = &mut delay => {
                    // timeout
                    if timeout_cnt < MAX_RESEND_COUNT{
                        // reset timer and resend
                        timeout_cnt += 1;
                        if self.rf.start(command).is_err() {
                            return Err(Error::NoLeader);
                        }
                        delay = tokio02::time::delay_for(Duration::from_millis(TIMEOUT_INTERVAL));
                    } else {
                        return Err(Error::Timeout);
                    }
                }
                result = &mut result_rx => {
                    return result.map_err(|_| Error::Others);
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
        info!("start a read operation");

        let command = Command {
            command_type: 3, // Get
            key: args.key,
            value: None,
            term: 0,
            token: 0,
        };
        let (tx, rx) = oneshot::channel::<ApplyResult>();
        let server = self.server.clone();
        // todo: use runtime.spawn()
        thread::spawn(move || {
            tx.send(
                tokio02_Runtime::new()
                    .unwrap()
                    .block_on(server.start_command(command)),
            )
            .unwrap_or_default();
        });
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

        info!("start a write operation");

        let command = Command {
            command_type: args.op,
            key: args.key,
            value: Some(args.value),
            term: 0,
            token: 0,
        };
        let (tx, rx) = oneshot::channel::<ApplyResult>();
        let server = self.server.clone();
        // todo: use runtime.spawn()
        thread::spawn(move || {
            tx.send(
                tokio02_Runtime::new()
                    .unwrap()
                    .block_on(server.start_command(command)),
            )
        });
        Box::new(rx.map_err(LError::Recv))
    }
}
