use log::info;
use tokio::process::{Child, Command};
use tokio::time::{Duration, sleep};
use tonic::Request;
use tonic::transport::Channel;

use chord::utils;
use chord::utils::crypto::HashPos;
use utils::crypto;

use crate::chord_proto::{Empty, NodeSummaryMsg, SuccessorListMsg};
use crate::chord_proto::chord_client::ChordClient;

pub mod chord_proto {
    tonic::include_proto!("chord");
}

const DURATION: Duration = Duration::from_secs(20 as u64);

#[tokio::main]
async fn main() {
    let mut node_summaries: Vec<NodeSummaryMsg> = Vec::new();
    {
        let node_ports = [
            // 5601,
            5602,
            5603,
            5604,
            // 5605,
            5606,
            // 5607,
            // 5611,
        ];
        // sleep(Duration::from_secs(20)).await;
        for node_port in node_ports {
            let mut client: ChordClient<Channel> = ChordClient::connect(format!("http://127.0.0.1:{}", node_port))
                .await
                .unwrap();
            let summary: NodeSummaryMsg = client.get_node_summary(Request::new(Empty {}))
                .await
                .unwrap().get_ref().clone();

            node_summaries.push(summary);
        }
        // child_handles getting out of scope will shut down nodes due to .kill_on_drop(true)
    }

    node_summaries.sort_by(|a: &NodeSummaryMsg, b: &NodeSummaryMsg| {
        a.pos.parse::<HashPos>().unwrap().cmp(&b.pos.parse::<HashPos>().unwrap())
    });

    let node_ids: Vec<HashPos> = node_summaries.iter()
        .map(|node_summary: &NodeSummaryMsg| node_summary.pos.parse::<HashPos>().unwrap())
        .collect::<Vec<HashPos>>();

    // check predecessors
    for i in 0..node_summaries.len() {
        let current_node: String = node_summaries[i].pos.clone();
        let next_node_pred: String = node_summaries[(i + 1) % node_summaries.len()].predecessor.clone().unwrap().id;

        if current_node.ne(&next_node_pred) {
            panic!("Node {} has wrong predecessor: {}", current_node, next_node_pred)
        }
    }

    // validate finger entries
    let mut is_valid = true;
    for i in 0..node_summaries.len() {
        let fingers = &node_summaries[i].finger_entries;
        for (j, finger) in fingers.iter().enumerate() {
            let finger_key: HashPos = finger.id.parse::<HashPos>().unwrap();
            let node_key_pointed_to = crypto::hash(&finger.address.as_bytes());
            let actually_responsible_node_key = get_responsible_node_for_key(finger_key, &node_ids);
            let actually_responsible_node_address = get_node_address_for_key(&actually_responsible_node_key, &node_summaries);
            if node_key_pointed_to.ne(&actually_responsible_node_key) {
                if is_valid {
                    eprintln!("-----");
                    is_valid = false;
                }
                eprintln!("Node ({}, {}): Wrong finger entry! ", node_summaries[i].pos, node_summaries[i].url);
                eprintln!("{}-th Finger {} points to node ({}, {}) ", j, finger_key, node_key_pointed_to, &finger.address);
                eprintln!("But node ({}, {}) is responsible for {}", actually_responsible_node_key, actually_responsible_node_address, finger_key);
                eprintln!("-----");
            }
        }
    }

    // validate predecessor list
    for (i, node_summary) in node_summaries.iter().enumerate() {
        let successor_list = node_summary.successor_list.as_ref();
        for (j, successor_according_to_list) in successor_list.unwrap().successors.iter().enumerate() {
            let actual_successor_address = &node_summaries[(i + j + 1) % node_summaries.len()].url;
            if successor_according_to_list.address.ne(actual_successor_address) {
                eprintln!("-----");
                eprintln!("Node ({}, {}): Wrong successor list! ", node_summaries[i].pos, node_summaries[i].url);
                eprintln!("Actual successor address: {}, but was {}", actual_successor_address, successor_according_to_list.address);
                eprintln!("-----");
                is_valid = false;
            }
        }
    }


    if is_valid {
        eprintln!("Looks good!")
    } else {
        eprintln!("Cluster is invalid!")
    }
}

fn get_responsible_node_for_key(key: HashPos, other_nodes: &Vec<HashPos>) -> HashPos {
    *other_nodes.iter()
        .filter(|&node| key <= *node)
        .min()
        .unwrap_or(other_nodes.iter().min().unwrap())
}

fn get_node_address_for_key(key: &HashPos, node_summaries: &Vec<NodeSummaryMsg>) -> String {
    node_summaries.iter()
        .find(|node_summary| node_summary.pos.parse::<HashPos>().unwrap().eq(key))
        .unwrap()
        .url
        .clone()
}

async fn start_up_nodes(node_count: usize) -> (Vec<u16>, Vec<Child>) {
    let mut child_handles = Vec::new();
    let mut ports = vec![5601_u16];

    let join_peer_address = format!("127.0.0.1:{}", ports[0]);

    // node 1 is the join peer for all other nodes
    let child_handle = get_base_node_start_up_command(5501u16, 5601u16, None);
    child_handles.push(child_handle);
    sleep(Duration::from_secs(2 as u64)).await;

    // all other nodes join node 1
    for i in 1..node_count {
        let grpc_node_port = 5601u16 + i as u16;
        let tcp_node_port = grpc_node_port - 100;
        let child_handle = get_base_node_start_up_command(
            tcp_node_port,
            grpc_node_port,
            Some(format!("127.0.0.1:{}", 5601u16 + i as u16 - 1).as_str()),
        );
        child_handles.push(child_handle);
        ports.push(grpc_node_port);

        info!("Started up node on port {}", grpc_node_port);
        sleep(DURATION).await;
    }
    (ports, child_handles)
}

fn get_base_node_start_up_command(tcp_node_port: u16, grpc_node_port: u16, peer_node_port: Option<&str>) -> Child {
    // todo: remove duplicate code here
    match peer_node_port {
        Some(peer) => {
            Command::new("cargo")
                .arg("run")
                .arg("--color=always")
                .args(&["--package", "chord"])
                .args(&["--bin", "chord"])
                .arg("--")
                .args(&["--tcp", format!("127.0.0.1:{}", tcp_node_port).as_str()])
                .args(&["--grpc", format!("127.0.0.1:{}", grpc_node_port).as_str()]).args(&["--peer", peer])
                .kill_on_drop(true)
                .spawn()
        }
        _ => {
            Command::new("cargo")
                .arg("run")
                .arg("--color=always")
                .args(&["--package", "chord"])
                .args(&["--bin", "chord"])
                .arg("--")
                .args(&["--tcp", format!("127.0.0.1:{}", tcp_node_port).as_str()])
                .args(&["--grpc", format!("127.0.0.1:{}", grpc_node_port).as_str()])
                .kill_on_drop(true)
                .spawn()
        }
    }
        .expect("failed to start process")
}
