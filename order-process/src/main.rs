// order-process (S2 cluster replica)
// Multi-machine: same .env everywhere; NODE_ID is auto-detected from local IP.
// Override: NODE_ID=1|2|3 ./starter.sh   (needed for all-localhost demos)

use order_process::config::{find_node, init_config, node_name, resolve_node_id};
use order_process::leader_election::LeaderElection;
use order_process::wal::ReplicatedCommand;
use rand::Rng;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashSet;
use std::net::UdpSocket;
use std::thread;

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
    // Increase OS receive buffer to 8 MB to absorb bursts during Raft consensus.
    // This reduces order drops when the leader is busy committing a batch.
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        unsafe {
            let fd = order_socket.as_raw_fd();
            let buf_size: libc::c_int = 8 * 1024 * 1024; // 8 MB
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &buf_size as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
    order_socket.set_nonblocking(true).ok();

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

        let mut orders: Vec<Order> = Vec::with_capacity(500);
        let mut seen_ids: HashSet<u64> = HashSet::with_capacity(500);
        while orders.len() < 500 {
            match order_socket.recv_from(&mut buf) {
                Ok((n, _src)) => {
                    if let Ok(order) = serde_json::from_slice::<Order>(&buf[..n]) {
                        // Deduplicate: same order_id may arrive from multiple sender threads.
                        if seen_ids.insert(order.order_id) {
                            orders.push(order);
                        }
                    }
                }
                Err(_) => break,
            }
        }

        if orders.is_empty() {
            thread::sleep(std::time::Duration::from_micros(200));
            continue;
        }

        if election.is_leader() {
            process_orders_batch_as_leader(node_id, &orders, &election, &result_socket);
        }
    }
}

fn process_orders_batch_as_leader(
    node_id: u8,
    orders: &[Order],
    election: &LeaderElection,
    result_socket: &UdpSocket,
) {
    let leader = node_name(node_id);
    let verbose = order_process::config::verbose_raft();
    let current_term = election.current_term();
    let outcomes = ["FILLED", "PARTIALLY_FILLED", "REJECTED"];
    let mut rng = rand::thread_rng();

    let commands: Vec<ReplicatedCommand> = orders
        .iter()
        .map(|order| {
            let status = outcomes[rng.gen_range(0..outcomes.len())];
            let filled_qty: u32 = if status == "REJECTED" {
                0
            } else {
                rng.gen_range(1..=order.qty)
            };

            ReplicatedCommand {
                order_id: order.order_id,
                symbol: order.symbol.clone(),
                side: order.side.clone(),
                qty: order.qty,
                status: status.to_string(),
                filled_qty,
                processed_by: format!("{} (S2-{})", leader, node_id),
                term: current_term,
            }
        })
        .collect();

    let committed_batch = election.propose_batch(commands);
    if committed_batch.is_empty() {
        return;
    }

    let cfg = order_process::config::config();
    for committed in committed_batch {
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

        if let Ok(buf) = serde_json::to_vec(&result) {
            let _ = result_socket.send_to(&buf, (cfg.s3_host.as_str(), cfg.s3_port));
        }

        if verbose {
            let line = result.to_string();
            println!(
                "[order] {} LEADER committed order_id={} status={} filled={}/{} -> S3 {}:{} {}",
                leader,
                committed.order_id,
                committed.status,
                committed.filled_qty,
                committed.qty,
                cfg.s3_host,
                cfg.s3_port,
                line
            );
        }
    }
}
