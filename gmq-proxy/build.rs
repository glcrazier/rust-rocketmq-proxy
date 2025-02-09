fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .out_dir("src/pb")
        .compile_protos(&["proto/apache/rocketmq/v2/service.proto"], &["proto"])?;
    Ok(())
}
