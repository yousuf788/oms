// order-monitoring — independent reachability arbiter for the order-process (S2) Raft
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
    println!("[order-monitoring] === STEP 1: Loading Configuration & Validating HMAC Key ===");
    let cfg = init_config();
    // Eagerly load monitoring_HMAC_KEY — panics with a clear message if not set.
    let _ = auth::monitoring_key();
    println!("[order-monitoring] monitoring_HMAC_KEY successfully validated");

    println!(
        "[order-monitoring] === STEP 2: Starting Health Polling Engine for Watched S2 Nodes ==="
    );
    println!(
        "[order-monitoring] Watching endpoints: {}",
        cfg.nodes
            .iter()
            .map(|n| format!("{} ({}:{})", n.name, n.host, n.health_port))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let table = start_health_poller();

    println!(
        "[order-monitoring] === STEP 3: Binding UDP Corroboration Listener on Port {} ===",
        cfg.monitoring_port
    );
    start_corroboration_responder(table); // blocks forever on the main thread
}
