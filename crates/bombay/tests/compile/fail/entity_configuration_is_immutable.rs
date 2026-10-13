fn main() {
    let mut configuration = bombay::entity::DirectoryConfig::default();
    configuration.shards = core::num::NonZeroUsize::new(3).unwrap();
}
