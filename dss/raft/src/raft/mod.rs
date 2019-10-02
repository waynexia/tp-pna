use std::default::Default;
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam::channel::{bounded, Receiver as CReceiver, Sender as CSender};
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

    pub last_applied: u64,
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
}

impl Default for State {
    fn default() -> Self {
        State {
            current_term: 0,
            voted_for: None,
            is_leader: false,
            leader_id: 0,
            last_applied: 0,
        }
    }
}

// states in Fig.4
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
    majority: usize, // number of majority
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
        _apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let (tx, rx) = bounded(1);
        let majority = (peers.len() + 1) / 2;
        let mut raft = Raft {
            peers,
            // persister,
            me,
            state: Arc::default(),
            tx,
            rx,
            majority,
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
                    tx.send(res).unwrap();
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
        &self,
        server: usize,
        args: &AppendEntriesArgs,
    ) -> Receiver<Result<AppendEntriesReply>> {
        let peer = &self.peers[server];
        let (tx, rx) = channel();
        peer.spawn(
            peer.append_entries(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    tx.send(res).unwrap();
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
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    pub fn start_state_machine(&self) {
        let action_interval = Duration::new(0, ACTION_INTERVAL);
        let mut clock: u16 = 0;

        let mut rng = rand::thread_rng();
        let election_timeout = rng.gen_range(MIN_TIMEOUT, MAX_TIMEOUT);

        let mut server_state = ServerStates::Follower;
        let (_, rx) = channel();
        let mut teller_rx: Receiver<bool> = rx;

        loop {
            match server_state {
                ServerStates::Follower => {
                    if clock > election_timeout {
                        let request_vote_args = RequestVoteArgs {
                            term: self.state.lock().unwrap().term(),
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
                            let mut cnt = 0;
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
                            tx.send(cnt >= majority).unwrap();
                        });

                        clock = 0;
                        server_state = ServerStates::Candidate;
                        teller_rx = rx;
                    }
                }

                ServerStates::Candidate => {
                    if clock > election_timeout {
                        let request_vote_args = RequestVoteArgs {
                            term: self.state.lock().unwrap().term(),
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
                            let mut cnt = 0;
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
                            tx.send(cnt >= majority).unwrap();
                        });

                        clock = 0;
                        teller_rx = rx;
                    }
                    if let Ok(result) = teller_rx.try_recv() {
                        if result {
                            server_state = ServerStates::Leader;
                            clock = 0;
                        }
                    }
                }

                ServerStates::Leader => {
                    if clock > HEARTBEAT_INTERVAL {
                        // construct append_entries_args
                        let append_entries_args = AppendEntriesArgs {
                            term: self.state.lock().unwrap().term(),
                            leader_id: self.me as u64,
                            prev_log_index: 0, //?
                            prev_log_term: 0,  //?
                            entries: vec![],
                            leader_commit: 0, //
                        };
                        for server_index in 0..self.peers.len() {
                            if server_index == self.me {
                                continue;
                            }
                            // how to use receiver returned by this?
                            self.send_append_entries(server_index, &append_entries_args);
                        }
                        clock = 0;
                    }
                }
            }

            // check clock for some timeout

            // try to receive heartbeat
            if !self.is_leader() {
                let signal = self.rx.try_recv();
                if signal.is_ok() && signal.unwrap() == IncomingRpcType::RequestVote {
                    clock = 0;
                }
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
        // let state = self.state;
        if self.state.lock().unwrap().current_term <= term
            && (self.state.lock().unwrap().voted_for.is_none()
                || self.state.lock().unwrap().voted_for == Some(candidate_id))
            && self.state.lock().unwrap().last_applied <= last_log_index
        {
            // Arc::get_mut(&mut self.state).unwrap().voted_for = Some(candidate_id);
            // state.voted_for.set(Some(candidate_id));
            // state.get_mut().vote_for = Some(candidate_id);
            self.state.lock().unwrap().voted_for = Some(candidate_id);

            return true;
        }
        false
    }

    /// try to append a entries. return the current_term in success, 0 in error.
    pub fn append_entries(&mut self, args: AppendEntriesArgs) -> u64 {
        // reset election timeout
        self.state.lock().unwrap().term() + args.entries.len() as u64
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
    raft: Arc<Mutex<Raft>>,
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
        let raft = Arc::new(Mutex::new(raft));

        let r = raft.clone();
        thread::spawn(move || {
            let raft = r.lock().unwrap();
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
        // self.raft.start(command)
        // crate::your_code_here(command)

        if !self.is_leader() {
            return Err(Error::NotLeader);
        }
        /* do something */
        // Ok(?,self.term())
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        crate::your_code_here(())
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        crate::your_code_here(())
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
        let raft = self.raft.lock().unwrap();
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
        let raft = self.raft.lock().unwrap();
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
        // let r = self.raft.clone();
        // let mut raft = self.raft.lock().unwrap();
        let mut raft = self.raft.lock().unwrap();

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
