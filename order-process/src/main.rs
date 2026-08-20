// order-process (S2 cluster replica)
// Multi-machine: same .env everywhere; NODE_ID is auto-detected from local IP.
// Override: NODE_ID=1|2|3 ./starter.sh   (needed for all-localhost demos)

use order_process::config::{find_node, init_config, node_name, resolve_node_id};
use order_process::leader_election::LeaderElection;
use order_process::wal::ReplicatedCommand;
use rand::Rng;
use serde::Deserialize;
use serde_json::json;
use std::net::UdpSocket;

#[derive(Deserialize, Debug)]
struct Order {
    order_id: u64,
    symbol: String,
    side: String,
    qty: u32,
}

fn main() {
    let cfg = init_config();
    let node_id = resolve_node_id();
    if !(1..=3).contains(&node_id) {
        panic!("NODE_ID must be 1, 2, or 3 (got {node_id})");
    }

    let self_node = find_node(node_id).expect("unknown NODE_ID");
    println!(
        "[role] {} (S2-{}) starting — peers: {}",
        node_name(node_id),
        node_id,
        cfg.nodes
            .iter()
            .map(|n| format!("{}={}", n.name, n.host))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let election = LeaderElection::start(node_id);
    let order_socket = UdpSocket::bind((cfg.bind_host.as_str(), self_node.order_port))
        .expect("failed to bind order channel");

    let result_socket =
        UdpSocket::bind((cfg.bind_host.as_str(), 0)).expect("failed to bind result socket");

    let mut last_role_line = String::new();
    let mut buf = [0u8; 4096];

    loop {
        let line = election.role_summary();
        if line != last_role_line {
            println!("[role] {line}");
            last_role_line = line;
        }

        order_socket
            .set_read_timeout(Some(std::time::Duration::from_millis(200)))
            .ok();
        let (n, _src) = match order_socket.recv_from(&mut buf) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let order: Order = match serde_json::from_slice(&buf[..n]) {
            Ok(o) => o,
            Err(_) => continue,
        };

        if election.is_leader() {
            process_order_as_leader(node_id, &order, &election, &result_socket);
        }
    }
}

fn process_order_as_leader(
    node_id: u8,
    order: &Order,
    election: &LeaderElection,
    result_socket: &UdpSocket,
) {
    let outcomes = ["FILLED", "PARTIALLY_FILLED", "REJECTED"];
    let mut rng = rand::thread_rng();
    let status = outcomes[rng.gen_range(0..outcomes.len())];
    let filled_qty: u32 = if status == "REJECTED" {
        0
    } else {
        rng.gen_range(1..=order.qty)
    };

    let command = ReplicatedCommand {
        order_id: order.order_id,
        symbol: order.symbol.clone(),
        side: order.side.clone(),
        qty: order.qty,
        status: status.to_string(),
        filled_qty,
        processed_by: format!("{} (S2-{})", node_name(node_id), node_id),
        term: election.current_term(),
    };

    if let Some(committed) = election.propose_command(command) {
        let result = json!({
            "order_id": committed.order_id,
            "symbol": committed.symbol,
            "side": committed.side,
            "qty": committed.qty,
            "status": committed.status,
            "filled_qty": committed.filled_qty,
            "processed_by": committed.processed_by,
            "term": committed.term,
        });

        let cfg = order_process::config::config();
        if let Ok(buf) = serde_json::to_vec(&result) {
            let _ = result_socket.send_to(&buf, (cfg.s3_host.as_str(), cfg.s3_port));
        }
    }
}
