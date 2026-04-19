fn main() {
    #[cfg(feature = "grpc")]
    {
        use std::env;
        use std::path::PathBuf;

        let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

        // Client-facing ledger API (package roda.ledger.v1).
        tonic_build::configure()
            .file_descriptor_set_path(out_dir.join("ledger_descriptor.bin"))
            .compile_protos(&["proto/ledger.proto"], &["proto"])
            .unwrap();

        // ADR-015: peer-to-peer replication API (package roda.node.v1).
        // Separate descriptor keeps it off the client-facing reflection
        // endpoint; the service is mounted on a different port in the
        // multi-node deployment (address added in ADR-016 wiring).
        tonic_build::configure()
            .file_descriptor_set_path(out_dir.join("node_descriptor.bin"))
            .compile_protos(&["proto/node.proto"], &["proto"])
            .unwrap();
    }
}
