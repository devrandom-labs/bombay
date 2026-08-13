#![no_main]

use libfuzzer_sys::fuzz_target;

#[path = "../../tests/support/mod.rs"]
mod support;

fuzz_target!(|bytes: &[u8]| {
    let order = support::permutation(bytes);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(support::accepted_payloads_are_processed_once(&order));
});
