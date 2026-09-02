use {
    crate::plugin::message::{ContactInfoMessage, MessageContactInfo},
    prost_types::Timestamp,
    std::time::SystemTime,
    yellowstone_grpc_proto::prelude as proto,
};

pub use yellowstone_grpc_convert::convert_to::*;

pub fn create_contact_info_node(
    node: &MessageContactInfo,
) -> proto::SubscribeUpdateContactInfoNode {
    proto::SubscribeUpdateContactInfoNode {
        pubkey: node.pubkey.to_bytes().into(),
        wallclock: node.wallclock,
        outset: node.outset,
        shred_version: node.shred_version as u32,
        version_major: node.version_major as u32,
        version_minor: node.version_minor as u32,
        version_patch: node.version_patch as u32,
        version_commit: node.version_commit,
        version_feature_set: node.version_feature_set,
        version_client_id: node.version_client_id as u32,
        gossip: node.gossip.map(|a| a.to_string()),
        tpu_quic: node.tpu_quic.map(|a| a.to_string()),
        tpu_forwards_quic: node.tpu_forwards_quic.map(|a| a.to_string()),
        tpu_vote_udp: node.tpu_vote_udp.map(|a| a.to_string()),
        tpu_vote_quic: node.tpu_vote_quic.map(|a| a.to_string()),
        tvu_udp: node.tvu_udp.map(|a| a.to_string()),
        tvu_quic: node.tvu_quic.map(|a| a.to_string()),
        serve_repair_udp: node.serve_repair_udp.map(|a| a.to_string()),
        serve_repair_quic: node.serve_repair_quic.map(|a| a.to_string()),
        rpc: node.rpc.map(|a| a.to_string()),
        rpc_pubsub: node.rpc_pubsub.map(|a| a.to_string()),
        alpenglow: node.alpenglow.map(|a| a.to_string()),
    }
}

pub fn create_gossip_update(
    seq: u64,
    message: &ContactInfoMessage,
) -> proto::SubscribeUpdateGossip {
    use proto::subscribe_update_gossip::UpdateOneof;

    let (update_oneof, created_at) = match message {
        ContactInfoMessage::Node(node) => (
            UpdateOneof::Node(create_contact_info_node(node)),
            node.created_at,
        ),
        ContactInfoMessage::Removed(removed) => (
            UpdateOneof::Removed(proto::SubscribeUpdateContactInfoRemoved {
                pubkey: removed.pubkey.to_bytes().into(),
            }),
            removed.created_at,
        ),
    };

    proto::SubscribeUpdateGossip {
        update_oneof: Some(update_oneof),
        created_at: Some(created_at),
        seq,
    }
}

pub fn create_gossip_ping() -> proto::SubscribeUpdateGossip {
    use proto::subscribe_update_gossip::UpdateOneof;

    proto::SubscribeUpdateGossip {
        update_oneof: Some(UpdateOneof::Ping(proto::SubscribeUpdatePing {})),
        created_at: Some(Timestamp::from(SystemTime::now())),
        seq: 0,
    }
}

pub fn create_gossip_snapshot(
    seq: u64,
    nodes: &[MessageContactInfo],
) -> proto::SubscribeUpdateGossip {
    use proto::subscribe_update_gossip::UpdateOneof;

    proto::SubscribeUpdateGossip {
        update_oneof: Some(UpdateOneof::Snapshot(proto::GossipTopology {
            nodes: nodes.iter().map(create_contact_info_node).collect(),
        })),
        created_at: Some(Timestamp::from(SystemTime::now())),
        seq,
    }
}
