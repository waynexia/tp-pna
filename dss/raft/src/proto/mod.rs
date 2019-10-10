pub mod raftpb {
    include!(concat!(env!("OUT_DIR"), "/raftpb.rs"));

    labrpc::service! {
        service raft {
            rpc request_vote(RequestVoteArgs) returns (RequestVoteReply);
            rpc append_entries(AppendEntriesArgs) returns (AppendEntriesReply);

            // Your code here if more rpc desired.
            // rpc xxx(yyy) returns (zzz)
        }
    }
    pub use self::raft::{
        add_service as add_raft_service, Client as RaftClient, Service as RaftService,
    };
}

pub mod kvraftpb {
    include!(concat!(env!("OUT_DIR"), "/kvraftpb.rs"));

    labrpc::service! {
        service kv {
            rpc get(GetRequest) returns (GetReply);
            rpc put_append(PutAppendRequest) returns (PutAppendReply);

            // Your code here if more rpc desired.
            // rpc xxx(yyy) returns (zzz)
        }
    }
    pub use self::kv::{add_service as add_kv_service, Client as KvClient, Service as KvService};
}

// A hand-writed message structure, since the usage of `optional` modifier isn't found in doc
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PresistentState {
    #[prost(uint64, tag = "1")]
    pub current_term: u64,
    #[prost(optional, uint64, tag = "2")]
    pub voted_for: Option<u64>,
    #[prost(bytes, repeated, tag = "3")]
    pub log: ::std::vec::Vec<std::vec::Vec<u8>>,
    #[prost(uint64, repeated, tag = "4")]
    pub log_term: ::std::vec::Vec<u64>,
}