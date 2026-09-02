# Rule: Environment Configuration & Secret Management

This rule specifies requirements for handling configuration, environment variables, secrets, and node ID resolution.

---

## 1. Zero Hardcoded Configuration Policy

- **Prohibition**: Hardcoding IP addresses, hostnames, ports, file paths, or HMAC secret keys in Rust source files is strictly prohibited.
- **`dotenvy` Integration**: All crates must load environment configurations using `dotenvy::dotenv()` inside their respective `config.rs` modules.
- **Fallback Hierarchy**: Use strict validation functions (`env_required`, `env_or`, `env_u16`, `env_u64`, `env_bool`) to load variables cleanly.

---

## 2. Mandatory Environment Variables

Every `.env` file in `order-process`, `order-sending`, `order-receiver`, and `order-witness` must define:

```env
# Cluster Host Addresses
NODE1_HOST=127.0.0.1
NODE2_HOST=127.0.0.1
NODE3_HOST=127.0.0.1

# Ports
NODE1_RAFT_PORT=6001
NODE2_RAFT_PORT=6002
NODE3_RAFT_PORT=6003

NODE1_ORDER_PORT=7001
NODE2_ORDER_PORT=7002
NODE3_ORDER_PORT=7003

NODE1_HEALTH_PORT=6101
NODE2_HEALTH_PORT=6102
NODE3_HEALTH_PORT=6103

S3_HOST=127.0.0.1
S3_PORT=8001

WITNESS_HOST=127.0.0.1
WITNESS_PORT=9101

# Security Keys
CLUSTER_HMAC_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
WITNESS_HMAC_KEY=fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210
```

---

## 3. `NODE_ID` Resolution Rules

1. **Explicit Override**: If `NODE_ID` environment variable is set (e.g. `NODE_ID=1`), use that ID directly.
2. **Auto-Detection**: If `NODE_ID` is unset, `config::resolve_node_id()` inspects active IPv4 network interface addresses on the local machine (`hostname -I`) and matches them against `NODE1_HOST`, `NODE2_HOST`, and `NODE3_HOST`.
3. **Ambiguity Prevention**: If multiple node hosts match local IPs (e.g. all set to `127.0.0.1` on local single-machine demos), auto-detection panics to force passing explicit `NODE_ID=1|2|3` or using `./starter.sh 1|2|3`.
