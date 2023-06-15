use tonic::Request;
use tonic::transport::Channel;

use chord::crypto;

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

    for i in 0..node_summaries.len() {
        if node_summaries[i].id.eq(&node_summaries[(i + 1) % node_summaries.len()].predecessor.clone().unwrap().id) {
            print!("error")
        }
    }
}
