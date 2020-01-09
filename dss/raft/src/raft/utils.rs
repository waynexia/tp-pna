use core::future::Future;

use futures03::channel::oneshot::Canceled;
use futures03::future::{select_all, FutureExt};

use super::errors::Error;
use crate::proto::raftpb::*;

pub async fn wait_vote_req_reply<V>(voters: V, majority: usize, candidate_term: u64) -> (bool, u64)
where
    V: IntoIterator,
    <V as IntoIterator>::Item: Future<Output = Result<Result<RequestVoteReply, Error>, Canceled>>,
    <V as IntoIterator>::Item: Unpin,
{
    let mut sth = select_all(voters.into_iter().map(|item| item.fuse()));
    // init to 1, stands for itself
    let mut cnt = 1;

    while cnt < majority {
        let (reply, _usize, voters) = sth.await;
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
        sth = select_all(voters);
    }
    (cnt >= majority, candidate_term)
}
