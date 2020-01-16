use std::default::Default;
// use std::future::Future as stdFuture;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
// use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam::channel::{bounded, Receiver as CReceiver, Sender as CSender};
use futures::sync::mpsc::UnboundedSender;
use futures::Future;
// use futures03::channel::oneshot::{channel as oneshot, Receiver as OReceiver};
// use futures03::future::FutureExt;
// use futures03::executor::block_on;
// use futures03::join;
use labrpc::RpcFuture;
use rand::Rng;
use tokio02::runtime::Runtime;
use tokio02::time::timeout;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;
mod utils;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use crate::proto::PresistentState;

const MIN_TIMEOUT: u16 = 150; // in millis
const MAX_TIMEOUT: u16 = 300;
const HEARTBEAT_INTERVAL: u16 = 100;
const APPEND_LISTEN_PERIOD: u16 = 100;
const ACTION_INTERVAL: u32 = 1_000_000; // in nano

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

type LogEntry = (Vec<u8>, u64);

/// State of a raft peer.
#[derive(Clone, Debug)]
pub struct State {
    pub current_term: Arc<AtomicU64>,
    pub voted_for: Arc<Mutex<Option<u64>>>,
    pub is_leader: Arc<AtomicBool>,
    pub leader_id: Arc<AtomicU64>,
    pub log: Arc<Mutex<Vec<LogEntry>>>,

    pub last_applied: Arc<AtomicU64>, // committed
    pub commit_index: Arc<AtomicU64>, // to be committed

    pub next_index: Arc<Mutex<Vec<u64>>>,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.current_term.load(Ordering::SeqCst)
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    pub fn reinit_next_index(&self) {
        let mut next_index = self.next_index.lock().unwrap();
        for index in &mut *next_index {
            *index = self.commit_index.load(Ordering::SeqCst);
        }
    }

    pub fn get_next_index(&self) -> Vec<u64> {
        self.next_index.lock().unwrap().to_vec()
    }

    pub fn get_voted_for(&self) -> Option<u64> {
        *self.voted_for.lock().unwrap()
    }

    pub fn get_log(&self) -> Vec<(Vec<u8>, u64)> {
        self.log.lock().unwrap().to_vec()
    }

    pub fn get_log_term_by_index(&self, index: u64) -> u64 {
        let log = self.get_log();
        if log.len() as u64 >= index && index != 0 {
            log[index as usize - 1].1
        } else {
            0
        }
    }

    pub fn increase_term(&self) {
        self.current_term.store(
            self.current_term.load(Ordering::SeqCst) + 1,
            Ordering::SeqCst,
        );
    }

    pub fn increase_last_applied(&self) {
        self.last_applied.store(
            self.last_applied.load(Ordering::SeqCst) + 1,
            Ordering::SeqCst,
        );
    }

    pub fn increase_commit_index(&self) {
        self.commit_index.store(
            self.commit_index.load(Ordering::SeqCst) + 1,
            Ordering::SeqCst,
        );
    }

    pub fn persist(&self, persister: Arc<Mutex<Box<dyn Persister>>>) {
        let mut data = vec![];

        // seperate log into two vector for presisting
        let mut sep_log = vec![];
        let mut sep_term = vec![];
        // last_applied means the last entries that has been replied to leader.
        let last_applied = self.last_applied.load(Ordering::SeqCst) as usize;
        let log = self.get_log()[..last_applied].to_vec();
        for (l, t) in log {
            sep_log.push(l.clone());
            sep_term.push(t);
        }

        let voted_for = self.get_voted_for();
        let presistent_state = PresistentState {
            current_term: self.term(),
            voted_for,
            log: sep_log,
            log_term: sep_term,
        };
        labcodec::encode(&presistent_state, &mut data).unwrap();
        persister.lock().unwrap().save_raft_state(data);

        // debug!("{} presisted!", self.me);
        debug!("presisted!");
    }

    pub fn adjust_next_index(&self, feedback: Vec<(usize, bool)>) {
        for (index, accept) in feedback {
            let mut next_index = self.next_index.lock().unwrap();
            if accept {
                next_index[index] = self.commit_index.load(Ordering::SeqCst);
            } else {
                let mut this_term = self.get_log_term_by_index(next_index[index]);
                let mut prev_term_index = next_index[index];
                // reach last log of prev term
                while prev_term_index > 0
                    && self.get_log_term_by_index(prev_term_index) == this_term
                {
                    prev_term_index -= 1;
                }
                this_term = self.get_log_term_by_index(prev_term_index);
                // get to the first log in this term
                while prev_term_index > 0
                    && self.get_log_term_by_index(prev_term_index - 1) == this_term
                {
                    prev_term_index -= 1;
                }
                next_index[index] = prev_term_index;
            }
            drop(next_index);
        }
    }

    pub fn new(num_peers: usize) -> State {
        let mut next_index = Vec::with_capacity(num_peers);
        for _ in 0..num_peers {
            next_index.push(0);
        }
        State {
            next_index: Arc::new(Mutex::new(next_index)),
            ..Default::default()
        }
    }
}

impl Default for State {
    fn default() -> Self {
        State {
            current_term: Arc::default(),
            voted_for: Arc::default(),
            is_leader: Arc::default(),
            log: Arc::default(),
            leader_id: Arc::default(),
            last_applied: Arc::default(),
            commit_index: Arc::default(),
            next_index: Arc::default(),
        }
    }
}

// states in Fig.4
#[derive(Debug)]
enum ServerStates {
    Follower,
    Candidate,
    Leader,
}

// Identify the notification from Node to Raft
#[derive(PartialEq)]
pub enum IncomingRpcType {
    AppendEntries,
    RequestVote(u64),
    TurnToFollower(u64),
    Stop,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Arc<Mutex<Box<dyn Persister>>>,
    // this peer's index into peers[]
    me: usize,
    state: State,
    // Transmitting message into state machine
    pub tx: CSender<IncomingRpcType>,
    rx: CReceiver<IncomingRpcType>,
    // number of majority
    majority: usize,
    // send message into this channel as applying command
    apply_ch: UnboundedSender<ApplyMsg>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        let (tx, rx) = bounded(1);
        let majority = (peers.len() + 1) / 2;
        let num_peers = peers.len();
        let raft = Raft {
            peers,
            persister: Arc::new(Mutex::new(persister)),
            me,
            state: State::new(num_peers),
            tx,
            rx,
            majority,
            apply_ch,
        };

        // initialize from state persisted before a crash
        raft.restore(&raft_state);

        raft
    }

    /// return self's term
    pub fn get_term(&self) -> u64 {
        self.state.term()
    }

    pub fn get_log(&self) -> Vec<(Vec<u8>, u64)> {
        self.state.log.lock().unwrap().to_vec()
    }

    /// restore previously persisted state.
    fn restore(&self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        match labcodec::decode::<PresistentState>(data) {
            Ok(presistent_state) => {
                debug!("data for restoring {} : {:?}", self.me, presistent_state);
                self.state
                    .current_term
                    .store(presistent_state.current_term, Ordering::SeqCst);
                let mut voted_for = self.state.voted_for.lock().unwrap();
                if let Some(voted) = presistent_state.voted_for {
                    *voted_for = Some(voted);
                } else {
                    *voted_for = None;
                }
                drop(voted_for);
                // construct tuple from two seperated vector
                let mut new_log = vec![];
                for index in 0..presistent_state.log.len() {
                    new_log.push((
                        presistent_state.log[index].clone(),
                        presistent_state.log_term[index],
                    ));
                }
                let mut log = self.state.log.lock().unwrap();
                *log = new_log;
                self.state
                    .last_applied
                    .store(log.len() as u64, Ordering::SeqCst);
                debug!("recovered state of peer {} : {:?}", self.me, self.state);
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
    }

    /// `start` is trying to commit a command, not start the state machine
    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if !self.is_leader() {
            return Err(Error::NotLeader);
        }
        let term = self.state.term();

        // send this command to state machine.
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        let mut log = self.state.log.lock().unwrap();
        let index = log.len() + 1;
        debug!(
            "{}, term: {}, buf : {:?}, command: {:?}",
            self.me,
            self.get_term(),
            buf,
            command
        );
        log.push((buf, term));

        Ok((index as u64, term))
    }

    /// stop background state machine thread
    pub fn stop(&self) {
        self.tx.send(IncomingRpcType::Stop).unwrap();
    }

    pub fn start_state_machine(&self) {
        let action_interval = Duration::new(0, ACTION_INTERVAL);
        let mut ticktock = Instant::now();
        let mut clock: u64 = 0;

        let mut rng = rand::thread_rng();
        let mut election_timeout = rng.gen_range(MIN_TIMEOUT, MAX_TIMEOUT);

        let server_state_lock = Arc::new(Mutex::new(ServerStates::Follower));
        let mut wating_vote_result = false;
        let need_send_heartbeat = Arc::new(AtomicBool::new(false));
        // let mut rt = Runtime::new().unwrap();

        // this raft instance is killed or not
        let mut killed = false;

        while !killed {
            let server_state = server_state_lock.lock().unwrap();
            match *server_state {
                ServerStates::Follower => {
                    // follower in waiting can be treated as candidate not timeout
                    drop(server_state);
                    let mut server_state = server_state_lock.lock().unwrap();
                    *server_state = ServerStates::Candidate;
                    drop(server_state);
                    continue;
                }

                ServerStates::Candidate => {
                    drop(server_state);
                    if clock > election_timeout as u64 {
                        // send request vote RPC
                        election_timeout = rng.gen_range(MIN_TIMEOUT, MAX_TIMEOUT);
                        self.state.increase_term();
                        let mut voted_for = self.state.voted_for.lock().unwrap();
                        *voted_for = Some(self.me as u64);
                        drop(voted_for);
                        // construct rpc
                        let last_log_term =
                            self.get_log().as_slice().last().unwrap_or(&(vec![], 0)).1;
                        let request_vote_args = RequestVoteArgs {
                            term: self.get_term(),
                            candidate_id: self.me as u64,
                            last_log_index: self.state.last_applied.load(Ordering::SeqCst),
                            last_log_term,
                        };
                        debug!("{} going to send vote request", self.me);

                        //copy variables
                        let majority = self.majority;
                        let candidate_term = self.state.current_term.load(Ordering::SeqCst);
                        let election_timeout_tomove = election_timeout;
                        let voted_for_lock = self.state.voted_for.clone();
                        let server_state_lock_ = server_state_lock.clone();
                        let need_send_heartbeat_to_move = need_send_heartbeat.clone();
                        let state = self.get_state();
                        let peers_tomove = self.peers.clone();

                        // sending & waiting future
                        let result = async move {
                            let listener = utils::wait_vote_req_reply(
                                peers_tomove,
                                request_vote_args,
                                majority,
                                candidate_term,
                            );
                            let result = timeout(
                                Duration::from_millis(election_timeout_tomove as u64),
                                listener,
                            )
                            .await;
                            if let Ok((success, term)) = result {
                                if success {
                                    let mut voted_for = voted_for_lock.lock().unwrap();
                                    *voted_for = None;
                                    drop(voted_for);
                                    let mut server_state = server_state_lock_.lock().unwrap();
                                    *server_state = ServerStates::Leader;
                                    drop(server_state);
                                    state.is_leader.store(true, Ordering::SeqCst);
                                    state.reinit_next_index();
                                    // set clock to timeout. send heartbeat once become leader
                                    need_send_heartbeat_to_move.store(true, Ordering::SeqCst);
                                } else if term > state.term() {
                                    state.current_term.store(term, Ordering::SeqCst);
                                }
                            }
                        };

                        // todo: use runtime.spawn()
                        thread::spawn(move || {
                            Runtime::new().unwrap().block_on(result);
                        });

                        // reset clock
                        clock = 0;
                    }
                }

                ServerStates::Leader => {
                    drop(server_state);
                    if clock > HEARTBEAT_INTERVAL as u64
                        || need_send_heartbeat.load(Ordering::SeqCst)
                    {
                        need_send_heartbeat.store(false, Ordering::SeqCst);
                        // send append entries (heartbeat) rpc
                        let term = self.get_term();
                        let leader_id = self.me;
                        let num_peers = self.peers.len();
                        let peers = self.peers.clone();
                        let majority = self.majority;
                        let next_index = self.state.get_next_index();
                        // let mut follower_tx = vec![];
                        let leader_commit = self.state.commit_index.load(Ordering::SeqCst);
                        let log = self.get_log();
                        let log_length = log.len();
                        self.state
                            .last_applied
                            .store(log_length as u64, Ordering::SeqCst);

                        // construct rpc and send to followers
                        let mut args = vec![];
                        for index in 0..num_peers {
                            if index == leader_id {
                                // placeholder
                                args.push(AppendEntriesArgs {
                                    term: 0,
                                    leader_id: 0,
                                    prev_log_index: 0,
                                    prev_log_term: 0,
                                    entries: vec![],
                                    entries_term: vec![],
                                    leader_commit: 0,
                                });
                                continue;
                            }

                            // construct entry list by `next_index` for every single peer
                            let prev_log_index = next_index[index];
                            let prev_log_term = self.get_log_term_by_index(prev_log_index);
                            let entries_to_send = if log_length > 0 {
                                log[next_index[index] as usize..log_length]
                                    .iter()
                                    .map(|(log, _)| log.clone())
                                    .collect()
                            } else {
                                vec![]
                            };
                            let entries_term_to_send = if log_length > 0 {
                                log[next_index[index] as usize..log_length]
                                    .iter()
                                    .map(|(_, term)| term.to_owned())
                                    .collect()
                            } else {
                                vec![]
                            };
                            args.push(AppendEntriesArgs {
                                term,
                                leader_id: leader_id as u64,
                                prev_log_index,
                                prev_log_term,
                                entries: entries_to_send,
                                entries_term: entries_term_to_send,
                                leader_commit,
                            });
                        }

                        let state = self.get_state();
                        let server_state_lock_ = server_state_lock.clone();
                        let me = self.me;
                        let apply_ch = self.apply_ch.clone();
                        let persister = self.persister.clone();
                        // let peers_tomove = self.peers.clone();
                        let result = async move {
                            let (success, term, feedback) = utils::wait_append_req_reply(
                                peers,
                                args,
                                // follower_tx,
                                majority,
                                state.term(),
                                APPEND_LISTEN_PERIOD as u64,
                            )
                            .await;
                            // adjust `nextIndex`
                            debug!("feedback: {:?}", feedback);
                            state.adjust_next_index(feedback);
                            debug!("{} received result: {}", me, success);
                            if success {
                                // apply log into apply_ch if log in this term is existing in majority
                                let log = state.get_log();
                                let mut have_new_commit = false;
                                if state.get_log_term_by_index(
                                    state.last_applied.load(Ordering::SeqCst),
                                ) == state.term()
                                {
                                    for command_index in state.commit_index.load(Ordering::SeqCst)
                                        ..state.last_applied.load(Ordering::SeqCst)
                                    {
                                        have_new_commit = true;
                                        let command = log[command_index as usize].0.clone();
                                        debug!(
                                            "leader {} will commit {:?} with index {}",
                                            me,
                                            command,
                                            command_index + 1
                                        );
                                        apply_ch
                                            .unbounded_send(ApplyMsg {
                                                command_valid: true,
                                                command,
                                                command_index: command_index + 1,
                                            })
                                            .unwrap();
                                        state.increase_commit_index();
                                    }
                                }

                                if have_new_commit {
                                    state.persist(persister);
                                }
                            } else if term > state.term() {
                                // turn to follower
                                debug!("leader {} received a append reply with term {}, which is greater than {}",me,term,state.term());
                                state.is_leader.store(false, Ordering::SeqCst);
                                let mut server_state = server_state_lock_.lock().unwrap();
                                *server_state = ServerStates::Follower;
                                drop(server_state);
                                state.current_term.store(term, Ordering::SeqCst);
                            }
                        };

                        // todo: use runtime.spawn()
                        thread::spawn(move || {
                            Runtime::new().unwrap().block_on(result);
                        });

                        // reset clock
                        clock = 0;
                    }
                }
            }

            self.listen_incomming_message(
                &mut killed,
                server_state_lock.clone(),
                &mut clock,
                &mut wating_vote_result,
                &mut election_timeout,
            );

            // step timer
            if ticktock.elapsed().as_millis() != 0 {
                clock += ticktock.elapsed().as_millis() as u64;
                ticktock = Instant::now();
            } else {
                // server isn't busy, sleep for a while
                thread::sleep(action_interval);
            }
        }
    }

    pub fn is_leader(&self) -> bool {
        self.state.is_leader()
    }

    pub fn get_state(&self) -> Arc<State> {
        Arc::new(self.state.clone())
    }

    // the first log's index is 1
    fn get_log_term_by_index(&self, index: u64) -> u64 {
        let log = self.get_log();
        if log.len() as u64 >= index && index != 0 {
            log[index as usize - 1].1
        } else {
            0
        }
    }

    /// try to vote for a candidate. If success, set state and return true, else return false
    pub fn vote_for(
        &self,
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) -> RpcFuture<RequestVoteReply> {
        let state = self.get_state();
        let tx = self.tx.clone();
        let me = self.me;
        let future = futures::future::result(Ok(true)).and_then(move |_| {
            let mut voted_for = state.voted_for.lock().unwrap();

            // first look at term, if the same then look at `voted_for`, otherwise need not.
            // after that look at index
            let self_last_log_term = state.get_log().as_slice().last().unwrap_or(&(vec![], 0)).1;
            let is_up_to_date = self_last_log_term < last_log_term // up to date
                            || (self_last_log_term == last_log_term
                                && state.last_applied.load(Ordering::SeqCst) <= last_log_index);
            debug!("self: {} (voted for:{:?}) received a vote request, candidate is {}, last log index is {}, own is {}  term is {}, own is {}, last log term is {}, its own is {}, up to date: {}",
                me,
                voted_for,candidate_id,
                last_log_index,
                state.last_applied.load(Ordering::SeqCst),
                term, state.term(),
                last_log_term,
                self_last_log_term,
                is_up_to_date);

            tx.send(IncomingRpcType::RequestVote(term)).unwrap_or_default();
            if ((state.term() == term
                && (voted_for.is_none() || *voted_for.as_ref().unwrap() == candidate_id))
                || state.term() < term)
                && is_up_to_date
            {
                // will grant
                *voted_for = Some(candidate_id);
                // leader step down
                // if state.term() < term {
                //     tx.send(IncomingRpcType::TurnToFollower(term)).unwrap_or_default();
                // }
                debug!("{}\tgrant", me);
                return Box::new(futures::future::result(Ok(RequestVoteReply {
                    term:state.term(),
                    vote_granted: true,
                })));
            }
            debug!("{}\tnot grant", me);
            Box::new(futures::future::result(Ok(RequestVoteReply {
                term:state.term(),
                vote_granted: false,
            })))
        });
        Box::new(future)
    }

    /// try to append a entries. return the current_term in success, 0 in error.
    pub fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        // if is not leader, send to apply_ch to commit,
        // if is really leader, will not run into this function
        // leader should call apply_ch in state machine

        let state = self.get_state();
        let tx = self.tx.clone();
        let me = self.me as u64;
        let persister = self.persister.clone();
        let apply_ch = self.apply_ch.clone();

        let future = futures::future::result(Ok(true)).and_then(move |_| {
            debug!(
                "No. {} (last applied: {}, term: {}) received {:?}",
                me,
                state.last_applied.load(Ordering::SeqCst),
                state.term(),
                args
            );
            if args.term >= state.current_term.load(Ordering::SeqCst) {
                tx.send(IncomingRpcType::TurnToFollower(args.term))
                    .unwrap_or_default();
                state.current_term.store(args.term, Ordering::SeqCst);
                let prev_log_term = state.get_log_term_by_index(args.prev_log_index);
                debug!(
                    "{} : arg.prev_index: {}, term: {}, self.prev_index: {}, term: {}",
                    me,
                    args.prev_log_index,
                    args.prev_log_term,
                    state.last_applied.load(Ordering::SeqCst),
                    prev_log_term
                );
                // return error to let leader decrease "next_index"
                if args.prev_log_index > state.last_applied.load(Ordering::SeqCst)
                    || (args.prev_log_term != prev_log_term && prev_log_term != 0)
                {
                    debug!("{} reject append entries rpc", me);
                    // return (state.term(), false);
                    return Box::new(futures::future::result(Ok(AppendEntriesReply {
                        term: state.term(),
                        success: false,
                        me,
                    })));
                }
                // if this rpc contains some entries that already exist in follower's log, follower's log
                // will be truncate first in following block, then write entries contained in rpc into follower's log
                else if args.prev_log_index < state.last_applied.load(Ordering::SeqCst) {
                    let mut log = state.log.lock().unwrap();
                    log.truncate(args.prev_log_index as usize);
                    state
                        .last_applied
                        .store(args.prev_log_index, Ordering::SeqCst);
                }
                // write entries from rpc into follower's log
                let mut log = state.log.lock().unwrap();
                for entry_index in 0..args.entries.len() {
                    log.push((
                        args.entries[entry_index].clone(),
                        args.entries_term[entry_index],
                    ));
                    state.increase_last_applied();
                    // self.state.increase_commit_index();
                }
                drop(log);
                if !args.entries.is_empty() {
                    state.persist(persister);
                }
                let log = state.get_log();
                let leader_commit = args.leader_commit;
                if state.get_log_term_by_index(leader_commit) == state.term() {
                    for log_index in state.commit_index.load(Ordering::SeqCst)..leader_commit {
                        debug!(
                            "follower {} will commit {:?} with index {}",
                            me,
                            log[log_index as usize],
                            log_index + 1,
                        );
                        apply_ch
                            .unbounded_send(ApplyMsg {
                                command_valid: true,
                                command: log[log_index as usize].0.clone(),
                                command_index: log_index + 1,
                            })
                            .unwrap();
                        state.increase_commit_index();
                    }
                }
                Box::new(futures::future::result(Ok(AppendEntriesReply {
                    term: state.term(),
                    success: true,
                    me,
                })))
            // (self.state.term(), true)
            } else {
                // figure 2, rule 1
                debug!("append entries rpc will return false");
                // (self.state.term(), false)
                Box::new(futures::future::result(Ok(AppendEntriesReply {
                    term: state.term(),
                    success: false,
                    me,
                })))
            }
        });

        Box::new(future)
    }

    fn listen_incomming_message(
        &self,
        killed: &mut bool,
        server_state_lock: Arc<Mutex<ServerStates>>,
        clock: &mut u64,
        wating_vote_result: &mut bool,
        _election_timeout: &mut u16,
    ) {
        // try to receive heartbeat
        while let Ok(signal) = self.rx.try_recv() {
            match signal {
                IncomingRpcType::Stop => {
                    *killed = true;
                }
                IncomingRpcType::RequestVote(incoming_term) => {
                    if !self.is_leader() {
                        let mut server_state = server_state_lock.lock().unwrap();
                        *server_state = ServerStates::Follower;
                        drop(server_state);
                        *wating_vote_result = false;
                        self.state
                            .current_term
                            .fetch_max(incoming_term, Ordering::SeqCst);
                    }
                }
                IncomingRpcType::TurnToFollower(incoming_term) => {
                    let mut server_state = server_state_lock.lock().unwrap();
                    *server_state = ServerStates::Follower;
                    drop(server_state);
                    debug!(
                        "leader {}, term: {} will turn to follower",
                        self.me,
                        self.get_term()
                    );
                    let mut voted_for = self.state.voted_for.lock().unwrap();
                    *voted_for = None;
                    drop(voted_for);
                    self.state.is_leader.store(false, Ordering::SeqCst);
                    self.state
                        .current_term
                        .fetch_max(incoming_term, Ordering::SeqCst);
                }
                // AppendEntries only need reset timer
                _ => {}
            }
            *clock = 0;
        }
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Raft>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let raft = Arc::new(raft);
        let r = raft.clone();
        thread::spawn(move || {
            let raft = r;
            raft.start_state_machine();
        });

        Node { raft }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // need to return immediately, so just calculate return value under
        // condition that "if success", and let raft state machine to really
        // commit these commands gracefully
        self.raft.start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.raft.get_term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.raft.is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> Arc<State> {
        let raft = self.raft.clone();
        raft.get_state()
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // need to stop state machine thread?
        self.raft.stop();
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // self.raft.tx.send(IncomingRpcType::RequestVote()).unwrap();
        self.raft.vote_for(
            args.term,
            args.candidate_id,
            args.last_log_index,
            args.last_log_term,
        )
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        self.raft.tx.send(IncomingRpcType::AppendEntries).unwrap();
        self.raft.append_entries(args)
    }
}
