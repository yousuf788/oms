#[derive(Clone, Copy, Debug)]
pub struct S2Node {
    pub host: &'static str,
    pub order_port: u16,
}

pub const S2_NODES: [S2Node; 3] = [
    S2Node {
        host: "127.0.0.1",
        order_port: 7001,
    },
    S2Node {
        host: "127.0.0.1",
        order_port: 7002,
    },
    S2Node {
        host: "127.0.0.1",
        order_port: 7003,
    },
];

pub const SENDER_BIND_HOST: &str = "0.0.0.0";
pub const SENDER_BIND_PORT: u16 = 9001;
