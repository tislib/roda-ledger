pub mod ledger {
    tonic::include_proto!("roda.ledger.v1");
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        include_bytes!(concat!(env!("OUT_DIR"), "/ledger_descriptor.bin"));
}

pub mod node {
    tonic::include_proto!("roda.node.v1");
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        include_bytes!(concat!(env!("OUT_DIR"), "/node_descriptor.bin"));
}
