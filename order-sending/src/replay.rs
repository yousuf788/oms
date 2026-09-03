// order-sending replay listener — serves REPLAY_REQUEST ranges from
// order-process nodes over a dedicated HMAC-signed UDP control channel
// (never the Aeron order stream — same separation-of-concerns pattern as
// the Raft control channel and monitoring corroboration channel elsewhere
// in this system). Replayed orders are re-published on the same per-node
// channel used for live orders, so on the wire they're indistinguishable
// from a live order — order-process's sequence tracker deduplicates them
// by order_id.
//
// Binds SENDER_BIND_PORT (the `bind_host`/`bind_port` fields in
// SenderConfig, previously unused).

use crate::auth;
use crate::wal::SenderWal;
use crate::OrderWire;
use serde::{Deserialize, Serialize};
use std::net::UdpSocket;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::thread;

/// Caps one response burst so a single request can't monopolize a node's
/// publisher thread indefinitely — the requester's periodic replay-request
/// ticker will simply ask again for whatever remains missing.
const MAX_ORDERS_PER_REQUEST: u64 = 20_000;

#[derive(Serialize, Deserialize, Debug)]
pub struct ReplayRequest {
    pub requester_id: u8,
    pub ranges: Vec<(u64, u64)>,
}

pub fn start_replay_listener(
    bind_host: String,
    bind_port: u16,
    wal: Arc<Mutex<SenderWal>>,
    node_channels: Vec<SyncSender<Arc<Vec<u8>>>>,
) {
    thread::spawn(move || {
        let socket = match UdpSocket::bind((bind_host.as_str(), bind_port)) {
            Ok(s) => s,
            Err(e) => {
                eprintln!(
                    "[order-sending] failed to bind replay listener on {bind_host}:{bind_port}: {e}"
                );
                return;
            }
        };
        println!("[order-sending] replay listener ready on {bind_host}:{bind_port}");

        let mut buf = [0u8; 8192];
        loop {
            let (n, src) = match socket.recv_from(&mut buf) {
                Ok(v) => v,
                Err(_) => continue,
            };

            let payload = match auth::verify(&buf[..n]) {
                Some(p) => p,
                None => {
                    eprintln!("[order-sending] dropping replay request from {src}: HMAC failure");
                    continue;
                }
            };
            let Ok(req) = bincode::deserialize::<ReplayRequest>(payload) else {
                continue;
            };
            let Some(node_tx) = node_channels.get((req.requester_id.saturating_sub(1)) as usize)
            else {
                eprintln!(
                    "[order-sending] replay request from unknown requester_id={}",
                    req.requester_id
                );
                continue;
            };

            let range_count = req.ranges.len();
            let mut served = 0u64;
            {
                let wal = wal.lock().unwrap();
                'ranges: for (from, to) in req.ranges {
                    let to = to.min(wal.last_order_id());
                    for order_id in from..=to {
                        if served >= MAX_ORDERS_PER_REQUEST {
                            break 'ranges;
                        }
                        let Some(order): Option<OrderWire> = wal.get(order_id) else {
                            continue;
                        };
                        let Ok(encoded) = bincode::serialize(&order) else {
                            continue;
                        };
                        let frame = Arc::new(auth::sign(&encoded));
                        if node_tx.send(frame).is_err() {
                            break 'ranges; // that node's publisher thread exited
                        }
                        served += 1;
                    }
                }
            }

            if served > 0 {
                println!(
                    "[order-sending] replayed {served} order(s) to node {} ({range_count} range(s) requested)",
                    req.requester_id
                );
            }
        }
    });
}
