#[derive(Clone, Copy, Debug)]
pub struct S2Node {
    pub host: &'static str,
    pub order_port: u16,
}

pub const S2_NODES: [S2Node; 3] = [
    S2Node {
        host: "172.16.12.104",
        order_port: 7001,
    },
    S2Node {
        host: "172.16.13.181",
        order_port: 7002,
    },
    S2Node {
        host: "10.10.1.121",
        order_port: 7003,
    },
];

pub const SENDER_BIND_HOST: &str = "0.0.0.0";
pub const SENDER_BIND_PORT: u16 = 9001;
