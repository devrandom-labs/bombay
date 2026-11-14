use bombay::ApplicationScope;
use bombay::behavior::Never;
use bombay::remote::Network;

#[path = "../../../../../examples/counter/src/counter.rs"]
#[expect(dead_code, reason = "reuse the actual Caller protocol dependencies")]
mod counter;
#[path = "../../../../../examples/remote-counter/src/deployment.rs"]
#[expect(dead_code, reason = "reuse actual operation and receipt types")]
mod deployment;
#[path = "../../../../../examples/remote-counter/src/protocol.rs"]
#[expect(
    dead_code,
    reason = "reuse the actual pure Caller and its request lane"
)]
mod protocol;

use protocol::{Caller, Journey, Requests, Results};

async fn configured(network: Network) {
    let scope = ApplicationScope::new();
    let requests = scope
        .external::<Requests>()
        .expect("actual local request port");
    let results = scope
        .external::<Results>()
        .expect("actual local result port");
    let caller = Caller {
        service: requests.recipient(),
        results: results.recipient(),
        journey: Journey::Increment,
    };
    let node = scope
        .with_root(caller)
        .open_node(network)
        .await
        .unwrap_or_else(|_| panic!("the supplied network opens"));
    // A local request port supplies no remote interpreter. Keep the same
    // explicit declaration seam as the compiling actual Client consumer.
    let mut node = node.exports(requests, async |application| {
        Ok::<_, Never>(application.root().established_recipient())
    });
    let outcome = node.run().await;
    drop((outcome, node, results));
}

fn main() {
    let _construction = configured;
}
