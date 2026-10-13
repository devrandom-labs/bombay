use bombay::entity::{DirectoryConfig, DirectoryConfigError, LocalDirectory};
use core::num::NonZeroUsize;

#[test]
fn shard_validation_matches_the_independent_small_count_table() {
    let accepted = [1, 2, 4, 8, 16, 32, 64, 128];
    for count in 1..=130 {
        for waiters in [1, 7, 63] {
            let shards = NonZeroUsize::new(count).expect("the range is positive");
            let activation_waiters = NonZeroUsize::new(waiters).expect("the table is positive");
            let configured = DirectoryConfig::new(shards, activation_waiters);
            match configured {
                Ok(configuration) => {
                    assert!(accepted.contains(&count));
                    assert_eq!(configuration.shards(), shards);
                    assert_eq!(configuration.activation_waiters(), activation_waiters);
                }
                Err(DirectoryConfigError::InvalidShardCount {
                    shards: original_shards,
                    activation_waiters: original_waiters,
                }) => {
                    assert!(!accepted.contains(&count));
                    assert_eq!(original_shards, shards);
                    assert_eq!(original_waiters, activation_waiters);
                }
            }
        }
    }
}

#[test]
fn rejected_limits_can_be_corrected_without_losing_waiter_capacity() {
    let configured = DirectoryConfig::new(
        NonZeroUsize::new(3).expect("positive"),
        NonZeroUsize::new(17).expect("positive"),
    );
    let Err(DirectoryConfigError::InvalidShardCount {
        shards,
        activation_waiters,
    }) = configured
    else {
        panic!("three shards cannot form a power-of-two hash mask");
    };
    assert_eq!(shards.get(), 3);
    assert_eq!(activation_waiters.get(), 17);
    let corrected =
        DirectoryConfig::new(NonZeroUsize::new(4).expect("positive"), activation_waiters)
            .expect("four shards are valid");
    assert_eq!(corrected.activation_waiters().get(), 17);
    let directory = LocalDirectory::<u64, Box<[u8]>, u64, u64>::new(corrected);
    assert_eq!(directory.len(), 0);
}

#[test]
fn default_configuration_installs_without_an_error_branch() {
    let configuration = DirectoryConfig::default();
    assert_eq!(configuration.shards().get(), 64);
    assert_eq!(configuration.activation_waiters().get(), 64);
    let directory = LocalDirectory::<u64, Box<[u8]>, u64, u64>::new(configuration);
    assert_eq!(directory.len(), 0);
}

#[test]
fn invalid_or_mutated_directory_configuration_cannot_be_forged() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/entity_configuration_fields_are_private.rs");
    cases.compile_fail("tests/compile/fail/entity_configuration_is_immutable.rs");
}
