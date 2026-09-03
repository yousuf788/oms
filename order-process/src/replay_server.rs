// order-process replay server for the S2(order-process)->S3(order-receiver)
// hop. Listens on this node's dedicated `replay_port` for HMAC-signed
// REPLAY_REQUEST messages from order-receiver (broadcast to all 3 S2 nodes,
// since order-receiver doesn't track Raft leadership). Only the current
// leader may act on one — followers must stay silent on the result channel,
// per this system's architecture invariant — so a valid request is simply
// enqueued onto a bounded channel that `LeaderElection::result_publisher_loop`
// drains; that keeps a single thread as the sole owner of the result Aeron
// publication, interleaving live commits with replay traffic rather than
// two threads calling `offer()` on it concurrently.

use crate::auth;
use crate::leader_election::LeaderElection;
use serde::{Deserialize, Serialize};
use std::net::UdpSocket;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::thread;

#[derive(Serialize, Deserialize, Debug)]
struct ReplayRequest {
    #[allow(dead_code)] // not needed to serve the request — S3 is the only result subscriber
    requester_id: u8,
    ranges: Vec<(u64, u64)>,
}

pub fn start_replay_server(
    bind_host: String,
    replay_port: u16,
    election: Arc<LeaderElection>,
    replay_tx: SyncSender<(u64, u64)>,
) {
    thread::spawn(move || {
        let socket = match UdpSocket::bind((bind_host.as_str(), replay_port)) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[replay-server] failed to bind on {bind_host}:{replay_port}: {e}");
                return;
            }
        };
        println!("[replay-server] listening on {bind_host}:{replay_port}");

        let mut buf = [0u8; 8192];
        loop {
            let (n, src) = match socket.recv_from(&mut buf) {
                Ok(v) => v,
                Err(_) => continue,
            };

            let payload = match auth::verify(&buf[..n]) {
                Some(p) => p,
                None => {
                    eprintln!("[replay-server] dropping request from {src}: HMAC failure");
                    continue;
                }
            };
            let Ok(req) = bincode::deserialize::<ReplayRequest>(payload) else {
                continue;
            };

            if !election.is_leader() {
                continue; // followers stay silent on the result channel
            }
            for range in req.ranges {
                let _ = replay_tx.try_send(range);
            }
        }
    });
}
