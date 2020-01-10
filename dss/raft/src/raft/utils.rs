use core::future::Future;
use std::time::Duration;

use futures03::channel::oneshot::Canceled;
use futures03::future::{select_all, FutureExt};
use tokio02::time::timeout;

use super::errors::Error;
use crate::proto::raftpb::*;

pub async fn wait_vote_req_reply<V>(voters: V, majority: usize, candidate_term: u64) -> (bool, u64)
where
    V: IntoIterator,
    <V as IntoIterator>::Item: Future<Output = Result<Result<RequestVoteReply, Error>, Canceled>>,
    <V as IntoIterator>::Item: Unpin,
{
    let mut selected = select_all(voters.into_iter().map(|item| item.fuse()));
    // init to 1, stands for itself
    let mut cnt = 1;

    while cnt < majority {
        let (reply, _, voters) = selected.await;
        if let Ok(Ok(result)) = reply {
            if result.vote_granted {
                cnt += 1;
            } else if result.term > candidate_term {
                return (false, result.term);
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
    mut ids: Vec<usize>,
    majority: usize,
    leader_term: u64,
    append_listen_period: u16,
) -> (bool, u64, Vec<(usize, bool)>)
where
    V: IntoIterator,
    <V as IntoIterator>::Item: Future<Output = Result<Result<AppendEntriesReply, Error>, Canceled>>,
    // <V as IntoIterator>::Item:
    //     Future<Output = Result<Result<Result<AppendEntriesReply, Error>, Canceled>, Elapsed>>,
    <V as IntoIterator>::Item: Unpin,
{
    let mut cnt = 1;
    // let mut ids = vec![];
    // let mut followers = vec![];
    // for (fut, id) in followers_arg {
    //     followers.push(fut.fuse());
    //     ids.push(id);
    // }
    // followers.map(|item| item.fuse());
    let mut selected = select_all(
        followers
            .into_iter()
            .map(|item| timeout(Duration::from_millis(append_listen_period as u64), item).fuse()),
    );
    let mut feedback = vec![];

    // while cnt < majority {
    loop {
        let (recv, index, followers) = selected.await;
        if let Ok(Ok(Ok(append_entries_reply))) = recv {
            // record follower's reply
            feedback.push((
                append_entries_reply.me as usize,
                append_entries_reply.success,
            ));
            ids.remove(index);

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
