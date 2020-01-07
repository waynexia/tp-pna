use std::default::Default;
// use std::future::Future as stdFuture;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam::channel::{bounded, Receiver as CReceiver, Sender as CSender};
use futures::sync::mpsc::UnboundedSender;
use futures::Future;
use futures03::channel::oneshot::{channel as oneshot, Receiver as OReceiver};
// use futures03::executor::block_on;
// use futures03::join;
use labrpc::RpcFuture;
use rand::Rng;
// use tokio02::runtime::Runtime;
// use tokio02::time::timeout;

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

const MIN_TIMEOUT: u16 = 200;
const MAX_TIMEOUT: u16 = 350;
const HEARTBEAT_INTERVAL: u16 = 150;
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
    RequestVote,
    TurnToFollower,
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

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&self) {
        let mut data = vec![];

        // seperate log into two vector for presisting
        let mut sep_log = vec![];
        let mut sep_term = vec![];
        // last_applied means the last entries that has been replied to leader.
        let last_applied = self.state.last_applied.load(Ordering::SeqCst) as usize;
        let log = self.get_log()[..last_applied].to_vec();
        for (l, t) in log {
            sep_log.push(l.clone());
            sep_term.push(t);
        }

        let voted_for = self.state.get_voted_for();
        let presistent_state = PresistentState {
            current_term: self.get_term(),
            voted_for,
            log: sep_log,
            log_term: sep_term,
        };
        labcodec::encode(&presistent_state, &mut data).unwrap();
        self.persister.lock().unwrap().save_raft_state(data);

        debug!("{} presisted!", self.me);
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

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: &RequestVoteArgs,
    ) -> OReceiver<Result<RequestVoteReply>> {
        let peer = &self.peers[server];
        let (tx, rx) = oneshot();
        peer.spawn(
            peer.request_vote(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    tx.send(res).unwrap_or_default(); // Supress Unused Result
                    Ok(())
                }),
        );
        rx
    }

    /// send append entries rpc
    fn send_append_entries(
        peers: &[RaftClient],
        server: usize,
        args: &AppendEntriesArgs,
    ) -> Receiver<Result<AppendEntriesReply>> {
        // use crate::proto::raftpb::raft::__futures::Future;

        let peer = &peers[server];
        let (tx, rx) = channel();
        peer.spawn(
            peer.append_entries(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    tx.send(res).unwrap_or_default(); // Supress Unused Result
                    Ok(())
                }),
        );
        rx
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
        let mut clock: u16 = 0;

        let mut rng = rand::thread_rng();
        let mut election_timeout = rng.gen_range(MIN_TIMEOUT, MAX_TIMEOUT);

        let mut server_state = ServerStates::Follower;
        let (_, rx) = channel();
        let mut teller_rx: Receiver<bool> = rx;
        let (_, rx) = channel();
        let mut append_entries_rx: Receiver<bool> = rx;
        let (append_entries_reply_tx, append_entries_reply_rx) = channel();
        let mut wating_vote_result = false;
        let mut wating_append_result = false;

        // this raft instance is killed or not
        let mut killed = false;

        while !killed {
            match server_state {
                ServerStates::Follower => {
                    server_state = ServerStates::Candidate;
                    continue;
                }

                ServerStates::Candidate => {
                    if clock > election_timeout {
                        // begin to send request vote RPC
                        self.state.increase_term();
                        let mut voted_for = self.state.voted_for.lock().unwrap();
                        *voted_for = Some(self.me as u64);
                        drop(voted_for);
                        let last_log_term =
                            self.get_log().as_slice().last().unwrap_or(&(vec![], 0)).1;
                        let request_vote_args = RequestVoteArgs {
                            term: self.get_term(),
                            candidate_id: self.me as u64,
                            last_log_index: self.state.last_applied.load(Ordering::SeqCst),
                            last_log_term,
                        };
                        let mut teller = vec![];
                        for server_index in 0..self.peers.len() {
                            if server_index == self.me {
                                continue;
                            }
                            teller.push(self.send_request_vote(server_index, &request_vote_args));
                        }

                        let (tx, rx) = channel();
                        let majority = self.majority;
                        let _candidate_term = self.state.current_term.clone();
                        // spawn a new thread to listen response from others and counts votes
                        thread::spawn(move || {
                            use tokio02::runtime::Builder;
                            use tokio02::time::timeout;
                            let mut rt = Builder::new().enable_time().build().unwrap();
                            debug!("{:?}", rt);
                            let listener = utils::wait_vote_req_reply(teller, majority);
                            let result = rt.block_on(async {
                                timeout(Duration::from_millis(HEARTBEAT_INTERVAL.into()), listener)
                                    .await
                            });
                            if let Ok(true) = result {
                                tx.send(true).unwrap();
                            } else {
                                tx.send(false).unwrap();
                            }

                            // let mut cnt = 1; // 1 for self
                            // while cnt < majority && !teller.is_empty() {
                            //     teller.retain(|rcv| {
                            //         if let Ok(response) = rcv.try_recv() {
                            //             if let Ok(request_vote_reply) = response {
                            //                 if request_vote_reply.vote_granted {
                            //                     cnt += 1;
                            //                     if cnt >= majority && tx.send(true).is_err() {}
                            //                 }
                            //                 // for what? should stop listen
                            //                 else if request_vote_reply.term
                            //                     > candidate_term.load(Ordering::SeqCst)
                            //                 {
                            //                     candidate_term.store(
                            //                         request_vote_reply.term,
                            //                         Ordering::SeqCst,
                            //                     );
                            //                 }
                            //             }
                            //             return false;
                            //         }
                            //         true
                            //     });
                            // }
                            // if tx.send(cnt >= majority).is_err() {}
                        });

                        clock = 0;
                        election_timeout = rng.gen_range(MIN_TIMEOUT, MAX_TIMEOUT);
                        teller_rx = rx;
                        wating_vote_result = true;
                    } else if wating_vote_result {
                        if let Ok(result) = teller_rx.try_recv() {
                            if result {
                                server_state = ServerStates::Leader;
                                let mut voted_for = self.state.voted_for.lock().unwrap();
                                *voted_for = None;
                                drop(voted_for);
                                self.state.is_leader.store(true, Ordering::SeqCst);
                                self.state.reinit_next_index();
                                clock = HEARTBEAT_INTERVAL + 1;
                            }
                            wating_vote_result = false;
                        }
                    }
                }

                ServerStates::Leader => {
                    if clock > HEARTBEAT_INTERVAL {
                        // for receiving ack from followers
                        let (tx, rx) = channel();
                        let term = self.get_term();
                        let leader_id = self.me;
                        let num_peers = self.peers.len();
                        let peers = self.peers.clone();
                        let majority = self.majority;

                        // adjust last_applied based on reply from last AppendEntriesRpc
                        if let Ok(feedback) = append_entries_reply_rx.try_recv() {
                            self.adjust_next_index(feedback);
                        }
                        let next_index = self.state.get_next_index();
                        let mut follower_tx = vec![];
                        let leader_commit = self.state.commit_index.load(Ordering::SeqCst);
                        let log = self.get_log();
                        let log_length = log.len();
                        self.state
                            .last_applied
                            .store(log_length as u64, Ordering::SeqCst);

                        for index in 0..num_peers {
                            if index == leader_id {
                                continue;
                            }

                            // construct entry list by `next_index` for every single peer
                            let prev_log_index = next_index[index];
                            // let prev_log_term = if prev_log_index > 0 {
                            //     self.get_log()[prev_log_index as usize - 1].1
                            // } else {
                            //     0
                            // };
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

                            let append_entries_args = AppendEntriesArgs {
                                term,
                                leader_id: leader_id as u64,
                                prev_log_index,
                                prev_log_term,
                                entries: entries_to_send,
                                entries_term: entries_term_to_send,
                                leader_commit,
                            };

                            follower_tx.push((
                                Raft::send_append_entries(
                                    &peers,
                                    index.to_owned(),
                                    &append_entries_args,
                                ),
                                index,
                            ));
                        }

                        // spawn a new thread to listen response from followers
                        let append_entries_reply_tx = append_entries_reply_tx.clone();
                        let tx = tx.clone();
                        // if a follower's reply contains a term greater than leader, send
                        // message to this channel to let this leader step down.
                        let step_down_tx = self.tx.clone();
                        let leader_term = self.state.current_term.clone();
                        thread::spawn(move || {
                            Raft::listen_follower_response(
                                follower_tx,
                                leader_term,
                                step_down_tx,
                                majority as u64,
                                tx,
                                action_interval,
                                append_entries_reply_tx,
                            )
                        });

                        clock = 0;
                        append_entries_rx = rx;
                        wating_append_result = true;
                    } else if wating_append_result {
                        if let Ok(result) = append_entries_rx.try_recv() {
                            debug!("{} received result: {}", self.me, result);
                            if result {
                                let log = self.get_log();
                                let mut have_new_commit = false;
                                // commit command(s) into apply_ch here if log in this term is
                                // existing in majority peers
                                if self.get_log_term_by_index(
                                    self.state.last_applied.load(Ordering::SeqCst),
                                ) == self.get_term()
                                {
                                    for command_index in
                                        self.state.commit_index.load(Ordering::SeqCst)
                                            ..self.state.last_applied.load(Ordering::SeqCst)
                                    {
                                        have_new_commit = true;
                                        let command = log[command_index as usize].0.clone();
                                        debug!(
                                            "leader {} will commit {:?} with index {}",
                                            self.me,
                                            command,
                                            command_index + 1
                                        );
                                        self.apply_ch
                                            .unbounded_send(ApplyMsg {
                                                command_valid: true,
                                                command,
                                                command_index: command_index + 1,
                                            })
                                            .unwrap();
                                        self.state.increase_commit_index();
                                    }
                                }

                                if have_new_commit {
                                    self.persist();
                                }
                                clock = 0;
                                wating_append_result = false;
                            }
                        }
                    }
                }
            }

            self.listen_incomming_message(
                &mut killed,
                &mut server_state,
                &mut clock,
                &mut wating_vote_result,
            );

            clock += 1;
            thread::sleep(action_interval);
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

    fn adjust_next_index(&self, feedback: Vec<(usize, bool)>) {
        for (index, accept) in feedback {
            let mut next_index = self.state.next_index.lock().unwrap();
            if accept {
                next_index[index] = self.state.commit_index.load(Ordering::SeqCst);
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

    /// try to vote for a candidate. If success, set state and return true, else return false
    pub fn vote_for(
        &self,
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) -> bool {
        let mut voted_for = self.state.voted_for.lock().unwrap();

        // first look at term, if the same then look at `voted_for`, otherwise need not.
        // after that look at index
        let self_last_log_term = self.get_log().as_slice().last().unwrap_or(&(vec![], 0)).1;
        let is_up_to_date = self_last_log_term < last_log_term // up to date
                    || (self_last_log_term == last_log_term
                        && self.state.last_applied.load(Ordering::SeqCst) <= last_log_index);
        debug!("self: {} (voted for:{:?}) received a vote request, candidate is {}, last log index is {}, own is {}  term is {}, own is {}, last log term is {}, its own is {}, up to date: {}",
            self.me,
            voted_for,candidate_id,
            last_log_index,
            self.state.last_applied.load(Ordering::SeqCst),
            term, self.state.term(),
            last_log_term,
            self_last_log_term,
            is_up_to_date);
        if ((self.state.term() == term
            && (voted_for.is_none() || *voted_for.as_ref().unwrap() == candidate_id))
            || self.state.term() < term)
            && is_up_to_date
        {
            *voted_for = Some(candidate_id);
            // leader step down
            if self.state.term() < term {
                self.tx
                    .send(IncomingRpcType::TurnToFollower)
                    .unwrap_or_default();
            }
            // self.state.current_term.store(term, Ordering::SeqCst);

            debug!("{}\tgrant", self.me);
            return true;
        }
        debug!("{}\tnot grant", self.me);
        self.state.current_term.store(term, Ordering::SeqCst);
        false
    }

    /// try to append a entries. return the current_term in success, 0 in error.
    pub fn append_entries(&self, args: AppendEntriesArgs) -> (u64, bool) {
        // if is not leader, send to apply_ch to commit,
        // if is really leader, will not run into this function
        // leader should call apply_ch in state machine

        debug!(
            "No. {} (last applied: {}, term: {}) received {:?}",
            self.me,
            self.state.last_applied.load(Ordering::SeqCst),
            self.get_term(),
            args
        );

        if args.term >= self.state.current_term.load(Ordering::SeqCst) {
            self.tx
                .send(IncomingRpcType::TurnToFollower)
                .unwrap_or_default();
            self.state.current_term.store(args.term, Ordering::SeqCst);

            let prev_log_term = self.get_log_term_by_index(args.prev_log_index);
            debug!(
                "{} : arg.prev_index: {}, term: {}, self.prev_index: {}, term: {}",
                self.me,
                args.prev_log_index,
                args.prev_log_term,
                self.state.last_applied.load(Ordering::SeqCst),
                prev_log_term
            );
            // return error to let leader decrease "next_index"
            if args.prev_log_index > self.state.last_applied.load(Ordering::SeqCst)
                || (args.prev_log_term != prev_log_term && prev_log_term != 0)
            {
                debug!("reject append entries rpc");
                return (self.state.term(), false);
            }
            // if this rpc contains some entries that already exist in follower's log, follower's log
            // will be truncate first in following block, then write entries contained in rpc into follower's log
            else if args.prev_log_index < self.state.last_applied.load(Ordering::SeqCst) {
                let mut log = self.state.log.lock().unwrap();
                log.truncate(args.prev_log_index as usize);
                self.state
                    .last_applied
                    .store(args.prev_log_index, Ordering::SeqCst);
            }

            // write entries from rpc into follower's log
            let mut log = self.state.log.lock().unwrap();
            for entry_index in 0..args.entries.len() {
                log.push((
                    args.entries[entry_index].clone(),
                    args.entries_term[entry_index],
                ));
                self.state.increase_last_applied();
                // self.state.increase_commit_index();
            }
            drop(log);
            if !args.entries.is_empty() {
                self.persist();
            }

            let log = self.get_log();
            let leader_commit = args.leader_commit;
            if self.get_log_term_by_index(leader_commit) == self.get_term() {
                for log_index in self.state.commit_index.load(Ordering::SeqCst)..leader_commit {
                    debug!(
                        "follower {} will commit {:?} with index {}",
                        self.me,
                        log[log_index as usize],
                        log_index + 1,
                    );
                    self.apply_ch
                        .unbounded_send(ApplyMsg {
                            command_valid: true,
                            command: log[log_index as usize].0.clone(),
                            command_index: log_index + 1,
                        })
                        .unwrap();
                    self.state.increase_commit_index();
                }
            }

            (self.state.term(), true)
        } else {
            // figure 2, rule 1
            debug!("append entries rpc will return false");
            (self.state.term(), false)
        }
    }

    // a method used in state machine
    fn listen_follower_response(
        mut follower_tx: Vec<(Receiver<Result<AppendEntriesReply>>, usize)>,
        leader_term: Arc<AtomicU64>,
        step_down_tx: CSender<IncomingRpcType>,
        majority: u64,
        tx: Sender<bool>,
        action_interval: std::time::Duration,
        append_entries_reply_tx: Sender<Vec<(usize, bool)>>,
    ) {
        let mut cnt = 1;
        let mut clock = 0;
        let mut reply = vec![];
        while !follower_tx.is_empty() {
            follower_tx.retain(|(rcv, index)| {
                if let Ok(response) = rcv.try_recv() {
                    if let Ok(append_entries_reply) = response {
                        if append_entries_reply.term
                            > leader_term.load(Ordering::SeqCst)
                        {
                            debug!("leader received a append reply with term {}, which is greater than {}",append_entries_reply.term,leader_term.load(Ordering::SeqCst));
                            leader_term.store(
                                append_entries_reply.term,
                                Ordering::SeqCst,
                            );
                            step_down_tx
                                .send(IncomingRpcType::TurnToFollower)
                                .unwrap_or_default();
                        }
                        if append_entries_reply.success {
                            cnt += 1;
                            if cnt >= majority{
                                tx.send(true).unwrap_or_default();
                            }
                        }
                        reply.push((
                            index.to_owned(),
                            append_entries_reply.success,
                        ));
                    }
                    return false;
                }
                true
            });

            // end for this time
            if clock > APPEND_LISTEN_PERIOD {
                break;
            }
            clock += 1;
            thread::sleep(action_interval);
        }
        tx.send(cnt >= majority).unwrap_or_default();
        append_entries_reply_tx.send(reply).unwrap_or_default();
    }

    fn listen_incomming_message(
        &self,
        killed: &mut bool,
        server_state: &mut ServerStates,
        clock: &mut u16,
        wating_vote_result: &mut bool,
    ) {
        // try to receive heartbeat
        while let Ok(signal) = self.rx.try_recv() {
            if signal == IncomingRpcType::Stop {
                *killed = true;
                break;
            }
            if !self.is_leader() {
                if signal == IncomingRpcType::RequestVote {
                    // need to judge term
                    *server_state = ServerStates::Follower;
                    *wating_vote_result = false;
                }
            } else if signal == IncomingRpcType::TurnToFollower {
                // need to judge term
                *server_state = ServerStates::Follower;
                debug!(
                    "leader {}, term: {} will turn to follower",
                    self.me,
                    self.get_term()
                );
                let mut voted_for = self.state.voted_for.lock().unwrap();
                *voted_for = None;
                drop(voted_for);
                self.state.is_leader.store(false, Ordering::SeqCst);
            }
            // and IncomingRpcType::AppendEntries
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
        let raft = self.raft.clone();
        let term = raft.get_term();

        if raft.vote_for(
            args.term,
            args.candidate_id,
            args.last_log_index,
            args.last_log_term,
        ) {
            raft.tx.send(IncomingRpcType::RequestVote).unwrap();
            Box::new(futures::future::result(Ok(RequestVoteReply {
                term,
                vote_granted: true,
            })))
        } else {
            Box::new(futures::future::result(Ok(RequestVoteReply {
                term,
                vote_granted: false,
            })))
        }
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let raft = self.raft.clone();

        let (term, success) = raft.append_entries(args);
        raft.tx.send(IncomingRpcType::AppendEntries).unwrap();
        Box::new(futures::future::result(Ok(AppendEntriesReply {
            term,
            success,
        })))
    }
}
