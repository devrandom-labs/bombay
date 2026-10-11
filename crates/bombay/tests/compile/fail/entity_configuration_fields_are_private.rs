fn main() {
    let _ = bombay::entity::DirectoryConfig {
        shards: core::num::NonZeroUsize::new(3).unwrap(),
        activation_waiters: core::num::NonZeroUsize::new(7).unwrap(),
    };
}
