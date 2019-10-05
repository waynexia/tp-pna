use std::default::Default;
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam::channel::{bounded, unbounded, Receiver as CReceiver, Sender as CSender};
use futures::sync::mpsc::UnboundedSender;
use futures::Future;
use labrpc::RpcFuture;
use rand::Rng;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

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

/// State of a raft peer.
#[derive(Clone, Debug)]
pub struct State {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub is_leader: bool,
    pub leader_id: u64,
    pub log: Vec<Vec<u8>>,

    pub last_applied: u64, // committed
    pub commit_index: u64, // to be committed

    pub next_index: Vec<u64>,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.current_term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }

    pub fn reinit_next_index(&mut self) {
        for index in &mut self.next_index {
            *index = self.last_applied;
        }
    }

    pub fn new(num_peers: usize) -> State {
        State {
            next_index: Vec::with_capacity(num_peers),
            ..Default::default()
        }
    }
}

impl Default for State {
    fn default() -> Self {
        State {
            current_term: 0,
            voted_for: None,
            is_leader: false,
            log: Vec::default(),
            leader_id: 0,
            last_applied: 0,
            commit_index: 0,
            next_index: Vec::default(),
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
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    // persister: Box<dyn Persister + Sync>,
    // this peer's index into peers[]
    me: usize,
    state: Arc<Mutex<State>>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    pub tx: CSender<IncomingRpcType>,
    rx: CReceiver<IncomingRpcType>,
    append_tx: CSender<Vec<u8>>,
    append_rx: CReceiver<Vec<u8>>,
    majority: usize, // number of majority
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

        // Your initialization code here (2A, 2B, 2C).
        let (tx, rx) = bounded(1);
        let (append_tx, append_rx) = unbounded();
        let majority = (peers.len() + 1) / 2;
        let num_peers = peers.len();
        let mut raft = Raft {
            peers,
            // persister,
            me,
            state: Arc::new(Mutex::new(State::new(num_peers))),
            tx,
            rx,
            append_tx,
            append_rx,
            majority,
            apply_ch,
        };

        // initialize from state persisted before a crash
        raft.restore(&raft_state);

        // crate::your_code_here((rf, apply_ch))
        raft
    }

    /// return self's term
    pub fn get_term(&self) -> u64 {
        self.state.lock().unwrap().term()
    }

    pub fn get_log(&self) -> Vec<Vec<u8>> {
        self.state.lock().unwrap().log.clone()
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
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
    ) -> Receiver<Result<RequestVoteReply>> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        let peer = &self.peers[server];
        let (tx, rx) = channel();
        peer.spawn(
            peer.request_vote(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    tx.send(res).unwrap_or_default(); // supress Unused Result
                    Ok(())
                }),
        );
        rx
        // ```
        // let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        // let reply = self.peers[server].request_vote(args).map_err(Error::Rpc);
        // tx.send(Ok(reply.wait()));
        // rx
    }

    /// send append entries rpc
    fn send_append_entries(
        peers: &[RaftClient],
        server: usize,
        args: &AppendEntriesArgs,
    ) -> Receiver<Result<AppendEntriesReply>> {
        let peer = &peers[server];
        let (tx, rx) = channel();
        peer.spawn(
            peer.append_entries(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    tx.send(res).unwrap_or_default(); // supress Unused Result
                    Ok(())
                }),
        );
        rx
    }

    /* `start` is trying to commit a command, not start the state machine */
    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here (2B).

        if !self.is_leader() {
            return Err(Error::NotLeader);
        }

        // construct return value pair
        let mut state = self.state.lock().unwrap();
        let index = state.commit_index + 1;
        let term = state.term();
        state.commit_index += 1;
        drop(state);

        // send this command to state machine.
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        self.append_tx.send(buf).unwrap();
        // self.apply_ch
        //     .unbounded_send(ApplyMsg {
        //         command_valid: true,
        //         command: buf,
        //         command_index: index,
        //     })
        //     .unwrap();

        Ok((index, term))
    }

    pub fn start_state_machine(&self) {
        let action_interval = Duration::new(0, ACTION_INTERVAL);
        let mut clock: u16 = 0;

        let mut rng = rand::thread_rng();
        let election_timeout = rng.gen_range(MIN_TIMEOUT, MAX_TIMEOUT);

        let mut server_state = ServerStates::Follower;
        let (_, rx) = channel();
        let mut teller_rx: Receiver<bool> = rx;
        let (_, rx) = channel();
        let mut append_entries_rx: Receiver<bool> = rx;
        let (append_entries_reply_tx, append_entries_reply_rx) = channel();
        let mut wating_vote_result = false;
        let mut wating_append_result = false;
        let mut entries = vec![];

        loop {
            match server_state {
                ServerStates::Follower => {
                    if clock > election_timeout {
                        // begin to send request vote RPC
                        let mut state = self.state.lock().unwrap();
                        state.current_term += 1;
                        drop(state);
                        let request_vote_args = RequestVoteArgs {
                            // term: self.state.lock().unwrap().term(),
                            term: self.get_term(),
                            candidate_id: self.me as u64,
                            last_log_index: 0, //?
                            last_log_term: 0,  //?
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
                        // spawn a new thread to listen response from others and counts votes
                        thread::spawn(move || {
                            let mut cnt = 1; // 1 for self
                            while cnt < majority && !teller.is_empty() {
                                teller.retain(|rcv| {
                                    if let Ok(response) = rcv.try_recv() {
                                        if let Ok(request_vote_reply) = response {
                                            if request_vote_reply.vote_granted {
                                                cnt += 1;
                                            }
                                        }
                                        return false;
                                    }
                                    true
                                });
                            }
                            if tx.send(cnt >= majority).is_err() {}
                        });

                        clock = 0;
                        server_state = ServerStates::Candidate;
                        teller_rx = rx;
                        wating_vote_result = true;
                    }
                }

                ServerStates::Candidate => {
                    if clock > election_timeout {
                        // begin to send request vote RPC
                        let mut state = self.state.lock().unwrap();
                        state.current_term += 1;
                        drop(state);
                        let request_vote_args = RequestVoteArgs {
                            // term: self.state.lock().unwrap().term(),
                            term: self.get_term(),
                            candidate_id: self.me as u64,
                            last_log_index: 0, //?
                            last_log_term: 0,  //?
                        };
                        let mut teller = vec![];
                        for server_index in 0..self.peers.len() {
                            if server_index == self.me {
                                continue;
                            }
                            teller.push(self.send_request_vote(server_index, &request_vote_args));
                        }
                        // need a vote timeout?
                        // do vote count

                        let (tx, rx) = channel();
                        let majority = self.majority;
                        thread::spawn(move || {
                            // spawn a new thread to listen response from others and counts votes
                            let mut cnt = 1; // 1 for self
                            while cnt < majority && !teller.is_empty() {
                                teller.retain(|rcv| {
                                    if let Ok(response) = rcv.try_recv() {
                                        if let Ok(request_vote_reply) = response {
                                            if request_vote_reply.vote_granted {
                                                cnt += 1;
                                            }
                                        }
                                        return false;
                                    }
                                    true
                                });
                            }
                            if tx.send(cnt >= majority).is_err() {}
                        });

                        clock = 0;
                        teller_rx = rx;
                        wating_vote_result = true;
                    }
                    if wating_vote_result {
                        if let Ok(result) = teller_rx.try_recv() {
                            if result {
                                server_state = ServerStates::Leader;
                                let mut state = self.state.lock().unwrap();
                                state.is_leader = true;
                                state.voted_for = None;
                                clock = 0;
                            }
                            wating_vote_result = false;
                        }
                    }
                }

                ServerStates::Leader => {
                    if clock > HEARTBEAT_INTERVAL {
                        // for ack from followers
                        let (tx, rx) = channel();

                        // gather entries need to replicate
                        while !self.append_rx.is_empty() {
                            entries.push(self.append_rx.recv().unwrap());
                        }

                        // spawn a new thread to indefinitely send AppendEntries RPC
                        let log = self.get_log();
                        // let entries_to_send = entries.clone();
                        let term = self.get_term();
                        let leader_id = self.me;
                        let num_peers = self.peers.len();
                        let peers = self.peers.clone();
                        let majority = self.majority;
                        let mut state = self.state.lock().unwrap();
                        let prev_log_index = state.last_applied;
                        // adjust last_applied based on reply from last AppendEntriesRpc
                        if let Ok(adjust) = append_entries_reply_rx.try_recv() {
                            for (index, accept) in adjust {
                                if accept {
                                    state.next_index[index] = state.last_applied;
                                } else {
                                    state.next_index[index] -= 1;
                                }
                            }
                        }
                        let next_index = state.next_index.clone();
                        drop(state);
                        let mut follower_tx = vec![];

                        // need not to spawn a new thread
                        for index in 0..num_peers {
                            if index == leader_id {
                                continue;
                            }

                            // construct entry list by `next_index` for every single peer
                            // will extries stay?
                            let entries_to_send = [
                                if log.len() > 0 {
                                    log[next_index[index] as usize..].to_vec()
                                } else {
                                    vec![]
                                },
                                entries.clone(),
                            ]
                            .concat();
                            let append_entries_args = AppendEntriesArgs {
                                term,
                                leader_id: leader_id as u64,
                                prev_log_index,      //?
                                prev_log_term: term, //?
                                entries: entries_to_send,
                                leader_commit: 0, //
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

                        // spawn a new thread to listen response from follower's response
                        let append_entries_reply_tx = append_entries_reply_tx.clone();
                        let tx = tx.clone();
                        // if a follower's reply contains a term greater than leader, send
                        // message to this channel to let this leader step down.
                        let step_down_tx = self.tx.clone();
                        let mut leader_term = self.state.lock().unwrap().current_term;
                        thread::spawn(move || {
                            let mut cnt = 1;
                            let mut clock = 0;
                            let mut reply = vec![];
                            while !follower_tx.is_empty() {
                                follower_tx.retain(|(rcv, index)| {
                                    if let Ok(response) = rcv.try_recv() {
                                        if let Ok(append_entries_reply) = response {
                                            if append_entries_reply.term > leader_term {
                                                leader_term = append_entries_reply.term;
                                                step_down_tx
                                                    .send(IncomingRpcType::TurnToFollower)
                                                    .unwrap_or_default();
                                            }
                                            if append_entries_reply.success {
                                                cnt += 1;
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
                        });

                        clock = 0;
                        append_entries_rx = rx;
                        wating_append_result = true;
                    }
                    if wating_append_result {
                        if let Ok(result) = append_entries_rx.try_recv() {
                            if result {
                                let mut state = self.state.lock().unwrap();
                                let mut log = state.log.clone();
                                // commit command(s) into apply_ch here
                                for entry in entries.clone() {
                                    self.apply_ch
                                        .unbounded_send(ApplyMsg {
                                            command_valid: true,
                                            command: entry.clone(),
                                            command_index: state.last_applied + 1,
                                        })
                                        .unwrap();
                                    log.push(entry);
                                    state.last_applied += 1;
                                }
                                state.log = log;
                                assert_eq!(state.log.len() as u64, state.last_applied + 1);
                                entries.clear();
                                clock = 0;
                                wating_append_result = false;
                            }
                        }
                    }
                }
            }

            // try to receive heartbeat
            if !self.is_leader() {
                if let Ok(signal) = self.rx.try_recv() {
                    if signal == IncomingRpcType::RequestVote {
                        // need to judge term
                        let mut state = self.state.lock().unwrap();
                        server_state = ServerStates::Follower;
                        state.voted_for = None;
                    }
                    clock = 0;
                }
            } else if let Ok(signal) = self.rx.try_recv() {
                if signal == IncomingRpcType::TurnToFollower {
                    // need to judge term
                    let mut state = self.state.lock().unwrap();
                    server_state = ServerStates::Follower;
                    state.voted_for = None;
                    state.is_leader = false;
                }
                clock = 0;
            }

            clock += 1;
            thread::sleep(action_interval);
        }
    }

    pub fn is_leader(&self) -> bool {
        // self.state.leader_id as usize == self.me
        self.state.lock().unwrap().is_leader()
    }

    pub fn get_state(&self) -> Arc<State> {
        // self.state.clone()
        use std::ops::Deref;
        Arc::new(self.state.lock().unwrap().deref().clone())
    }

    /// try to vote for a candidate. If success, set state and return true, else return false
    pub fn vote_for(&self, term: u64, candidate_id: u64, last_log_index: u64) -> bool {
        let s = self.state.clone();
        let mut state = s.lock().unwrap();
        if state.current_term <= term
            && (state.voted_for.is_none() || state.voted_for == Some(candidate_id))
            && state.last_applied <= last_log_index
        {
            // Arc::get_mut(&mut self.state).unwrap().voted_for = Some(candidate_id);
            // state.voted_for.set(Some(candidate_id));
            // state.get_mut().vote_for = Some(candidate_id);
            state.voted_for = Some(candidate_id);

            return true;
        }
        false
    }

    /// try to append a entries. return the current_term in success, 0 in error.
    pub fn append_entries(&self, args: AppendEntriesArgs) -> u64 {
        // if not leader, send to apply_ch to commit,
        // if is really leader, will not run into this function
        // leader should call apply_ch in state machine

        let mut state = self.state.lock().unwrap();
        let mut log = state.log.clone();

        if args.term >= state.current_term {
            self.tx
                .send(IncomingRpcType::TurnToFollower)
                .unwrap_or_default();
            state.current_term = args.term;

            // no action with commends that already applied.
            if args.prev_log_index < state.last_applied {
                return state.current_term;
            } else if args.prev_log_index > state.last_applied {
                return 0;
            }

            for entry in args.entries {
                self.apply_ch
                    .unbounded_send(ApplyMsg {
                        command_valid: true,
                        command: entry.clone(),
                        command_index: state.last_applied + 1,
                    })
                    .unwrap();
                log.push(entry);
                state.last_applied += 1;
            }
            state.log = log;
            assert_eq!(state.log.len() as u64, state.last_applied);
            state.current_term
        } else {
            0
        }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(0, &Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        // let _ = &self.persister;
        let _ = &self.peers;
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
    // raft: Raft,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        /* some code to spawn a new thread for ratf to run */
        // thread::spawn(|| {
        //     raft.start_state_machine();
        // });
        // Your code here.
        // crate::your_code_here(raft)
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
        // Your code here.
        // Example:

        // need to return immediately, so just calculate return value under
        // condition that "if success", and let raft state machine to really
        // commit these commands gracefully
        self.raft.start(command)

        // if !self.is_leader() {
        //     return Err(Error::NotLeader);
        // }
        /* do something */
        // Ok(?,self.term())
        // crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        // crate::your_code_here(())
        self.raft.get_term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        // crate::your_code_here(())
        self.raft.is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> Arc<State> {
        // State {
        //     current_term: self.term(),
        //     is_leader: self.is_leader(),
        //     votedFor: None,
        //     leader_id: self.raft.
        // }

        // let raft_locked = self.raft.clone();
        // let raft = raft_locked.lock().unwrap();
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
        // Your code here, if desired.
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        // crate::your_code_here(args)

        // let r = self.raft.clone();
        // let mut raft = r.lock().unwrap();
        let raft = self.raft.clone();
        let term = raft.get_term();

        if raft.vote_for(args.term, args.candidate_id, args.last_log_index) {
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

        let term = raft.append_entries(args);
        if term > 0 {
            raft.tx.send(IncomingRpcType::AppendEntries).unwrap();
            Box::new(futures::future::result(Ok(AppendEntriesReply {
                term,
                success: true,
            })))
        } else {
            Box::new(futures::future::result(Ok(AppendEntriesReply {
                term: 0,
                success: false,
            })))
        }
    }
}
