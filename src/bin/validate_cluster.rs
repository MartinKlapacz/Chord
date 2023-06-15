use tonic::Request;
use tonic::transport::Channel;

use chord::crypto;
use chord::crypto::Key;

use crate::chord_proto::{Empty, NodeSummaryMsg};
use crate::chord_proto::chord_client::ChordClient;

pub mod chord_proto {
    tonic::include_proto!("chord");
}

#[tokio::main]
async fn main() {
    let node_ports: Vec<i32> = vec![5601];
    let mut node_summaries: Vec<NodeSummaryMsg> = Vec::new();

    for node_port in node_ports {
        let mut client: ChordClient<Channel> = ChordClient::connect(format!("http://127.0.0.1:{}", node_port))
            .await
            .unwrap();
        let summary: NodeSummaryMsg = client.get_node_summary(Request::new(Empty {}))
            .await
            .unwrap().get_ref().clone();

        node_summaries.push(summary);
    }

    node_summaries.sort_by(|a: &NodeSummaryMsg, b: &NodeSummaryMsg| {
        a.id.parse::<u128>().unwrap().cmp(&b.id.parse::<u128>().unwrap())
    });

    let node_ids: Vec<Key> = node_summaries.iter()
        .map(|node_summary: &NodeSummaryMsg| node_summary.id.parse::<Key>().unwrap())
        .collect::<Vec<Key>>();

    // check predecessors
    for i in 0..node_summaries.len() {
        let current_node: String = node_summaries[i].id.clone();
        let next_node_pred: String = node_summaries[(i + 1) % node_summaries.len()].predecessor.clone().unwrap().id;

        if current_node.ne(&next_node_pred) {
            panic!("Node {} has wrong predecessor: {}", current_node, next_node_pred)
        }
    }

    for i in 0..node_summaries.len() {
        let fingers = &node_summaries[i].finger_entries;
        for finger in fingers {
            let finger_key: Key = finger.id.parse::<Key>().unwrap();
            let node_pointed_to = crypto::hash(&finger.address);
            let actually_responsible_node = get_responsible_node_for_key(finger_key, &node_ids);
            assert_eq!(node_pointed_to, actually_responsible_node)
        }
    }

    println!("looks good!")
}

fn get_responsible_node_for_key(key: Key, other_nodes: &Vec<Key>) -> Key {
    *other_nodes.iter()
        .filter(|&node| key < *node)
        .min()
        .unwrap_or(other_nodes.iter().min().unwrap())
}
