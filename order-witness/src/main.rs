// order-witness — independent reachability arbiter for the order-process (S2) Raft
// cluster. Non-sequencing: never processes orders, never becomes leader, never holds
// consensus state. It only answers one question for an isolated node: "can you reach
// my peers right now?" — see corroboration.rs for the decision rule.

mod auth;
mod config;
mod corroboration;
mod health_poll;

use config::init_config;
use corroboration::start_corroboration_responder;
use health_poll::start_health_poller;

fn main() {
    let cfg = init_config();
    // Eagerly load WITNESS_HMAC_KEY — panics with a clear message if not set.
    let _ = auth::witness_key();
    println!(
        "[witness] starting — watching: {}",
        cfg.nodes
            .iter()
            .map(|n| format!("node{}={}:{}", n.id, n.host, n.health_port))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let table = start_health_poller();
    start_corroboration_responder(table); // blocks forever on the main thread
}
