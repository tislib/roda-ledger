fn main() {
    #[cfg(feature = "grpc")]
    {
        use std::env;
        use std::path::PathBuf;

        let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
        tonic_build::configure()
            .file_descriptor_set_path(out_dir.join("ledger_descriptor.bin"))
            .compile_protos(&["proto/ledger.proto"], &["proto"])
            .unwrap();
        tonic_build::configure()
            .file_descriptor_set_path(out_dir.join("node_descriptor.bin"))
            .compile_protos(&["proto/node.proto"], &["proto"])
            .unwrap();
    }
}
