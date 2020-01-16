use core::future::Future as FutureStd;
use std::time::Duration;

use futures::Future;
use futures03::channel::oneshot::Canceled;
use futures03::channel::oneshot::{channel as oneshot, Receiver};
use futures03::future::{select_all, FutureExt};
use tokio02::time::timeout;

use super::errors::Error;
use crate::proto::raftpb::*;

/// Try to send request vote rpc to other peers and waiting for response.
/// return vote result to caller.
///
/// This future will return as soon as granted counter is greater than
/// majority. But before that, if a rpc timeout it will try to resend;
/// if is blocksd it will keep waiting. So it is better to add a timeout
/// restrict on this future.
pub async fn wait_vote_req_reply(
    peers: Vec<RaftClient>,
    rpc_arg: RequestVoteArgs,
    majority: usize,
    candidate_term: u64,
) -> (bool, u64) {
    let me = rpc_arg.candidate_id as usize;
    let mut voters = vec![];
    for (peer_index, peer) in peers.iter().enumerate() {
        if peer_index == me {
            continue;
        }
        voters.push(send_request_vote(&peer, &rpc_arg, peer_index).fuse());
    }
    let mut selected = select_all(voters);

    // init to 1, stands for itself
    let mut cnt = 1;

    while cnt < majority {
        let (reply, _, mut voters) = selected.await;
        debug!("received reply:{:?} at term {}", reply, candidate_term);
        if let Ok(result) = reply {
            match result {
                // This rpc is successfully returned.
                Ok(vote_reply) => {
                    if vote_reply.vote_granted {
                        cnt += 1;
                    } else if vote_reply.term > candidate_term {
                        return (false, vote_reply.term);
                    }
                }
                // The rpc is lost, try to resend
                Err(Error::NeedResend(resend_index)) => {
                    voters.push(
                        send_request_vote(&peers[resend_index], &rpc_arg, resend_index).fuse(),
                    );
                }
                _ => {}
            }
        }
        if voters.is_empty() {
            break;
        }
        selected = select_all(voters);
    }
    (cnt >= majority, candidate_term)
}

/// Unlike wating for vote request (return once counter is equal or greater than majority),
/// here we need to collect followers' response as much as we can because the result
/// replied by follower is needed when adjusting `next_index`.
///
/// return values:
/// success or not, new term if needed, follower's feedback on `prevLogTerm`
pub async fn wait_append_req_reply<V>(
    followers: V,
    majority: usize,
    leader_term: u64,
    append_listen_period: u16,
) -> (bool, u64, Vec<(usize, bool)>)
where
    V: IntoIterator,
    <V as IntoIterator>::Item:
        FutureStd<Output = Result<Result<AppendEntriesReply, Error>, Canceled>>,
    // <V as IntoIterator>::Item:
    //     Future<Output = Result<Result<Result<AppendEntriesReply, Error>, Canceled>, Elapsed>>,
    <V as IntoIterator>::Item: Unpin,
{
    let mut cnt = 1;
    let mut selected = select_all(
        followers
            .into_iter()
            .map(|item| timeout(Duration::from_millis(append_listen_period as u64), item).fuse()),
    );
    let mut feedback = vec![];

    loop {
        let (recv, _, followers) = selected.await;
        if let Ok(Ok(Ok(append_entries_reply))) = recv {
            // record follower's feedback
            feedback.push((
                append_entries_reply.me as usize,
                append_entries_reply.success,
            ));

            if append_entries_reply.success {
                cnt += 1;
            } else if append_entries_reply.term > leader_term {
                // leader is illegal. can return immediately
                return (false, append_entries_reply.term, vec![]);
            }
        }
        if followers.is_empty() {
            break;
        }
        selected = select_all(followers);
    }
    (cnt >= majority, leader_term, feedback)
}

/// Send request vote rpc. If a rpc is lost it will return a error contains
/// peer number that need to resend
fn send_request_vote(
    peer: &RaftClient,
    args: &RequestVoteArgs,
    peer_index: usize,
) -> Receiver<Result<RequestVoteReply, Error>> {
    let (tx, rx) = oneshot();
    peer.spawn(
        peer.request_vote(&args)
            .map_err(Error::Rpc)
            .then(move |res| {
                tx.send(res.map_err(|_| Error::NeedResend(peer_index)))
                    .unwrap_or_default(); // Supress Unused Result
                Ok(())
            }),
    );
    rx
}
