use bombay::ApplicationScope;
use bombay::MailAddr;
use bombay::actors::ActorExt;
use bombay::behavior::{Never, User};
use bombay::remote::{Client, ClientLimits, Network, json};
use futures_util::future::Either;
use std::time::Duration;

#[path = "../../../../../examples/counter/src/counter.rs"]
#[expect(
    dead_code,
    reason = "the fixture reuses the actual nominal Counter protocol"
)]
mod counter;
#[path = "../../../../../examples/remote-counter/src/deployment.rs"]
#[expect(
    dead_code,
    reason = "the fixture consumes an already validated actual finite deployment"
)]
mod deployment;
#[path = "../../../../../examples/remote-counter/src/protocol.rs"]
#[expect(
    dead_code,
    reason = "the fixture type-checks the actual caller and wire functions"
)]
mod protocol;

use deployment::{Deployment, Operation, Purpose, Receipt};
use protocol::{Caller, Journey, Requests, Results};

async fn configured(
    network: Network,
    configured: &Deployment,
    counts: ClientLimits,
    framing: json::Limits,
    wait: Duration,
) {
    let scope = ApplicationScope::new();
    let requests = scope.external::<Requests>().expect("actual request source");
    let results = scope
        .external::<Results>()
        .expect("actual domain result port");
    let caller = Caller {
        service: requests.recipient(),
        results: results.recipient(),
        journey: Journey::Increment,
    }
    .with_receive_timeout(wait, Caller::expire);
    let node = scope
        .with_root(caller)
        .open_node(network)
        .await
        .unwrap_or_else(|original| {
            drop(original);
            panic!("the supplied native configuration opens")
        });
    let client = Client::new(
        requests,
        protocol::KEY,
        counts,
        framing,
        wait,
        |original: &User<MailAddr, Operation>, _: &str, _: MailAddr| {
            let grant = configured
                .metadata(original.message)
                .map_err(Either::Left)?;
            protocol::request(grant, original.message)
                .map(Some)
                .map_err(Either::Right)
        },
        async |bytes: &[u8], _: &User<MailAddr, Operation>| {
            configured.protect(Purpose::Request, bytes)
        },
        async |bytes: &[u8], proof: &[u8]| configured.verify(Purpose::Reply, bytes, proof),
        |receipt: &Receipt, bytes: &[u8], original: &User<MailAddr, Operation>| {
            let parsed: protocol::Response =
                serde_json::from_slice(bytes).map_err(|cause| protocol::Refusal::Json { cause })?;
            if parsed.bombay != *receipt.metadata()
                || parsed.kind != Purpose::Reply
                || parsed.operation != receipt.operation()
                || original.message != parsed.operation
            {
                return Err(protocol::Refusal::Scope);
            }
            Ok(parsed.bombay.request_sequence)
        },
        |original: &User<MailAddr, Operation>, runtime: &str, target: MailAddr| {
            if original.from == target
                && configured.caller().runtime == runtime
                && configured.caller().actor == target.0
            {
                Ok(())
            } else {
                Err(protocol::Refusal::Scope)
            }
        },
        |receipt: &Receipt,
         original: &User<MailAddr, Operation>,
         runtime: &str,
         target: MailAddr| {
            let grant = configured.metadata(receipt.operation())?;
            if original.from == target
                && configured.caller().runtime == runtime
                && configured.caller().actor == target.0
                && receipt.metadata() == grant
                && original.message == receipt.operation()
            {
                Ok(())
            } else {
                Err(deployment::Refusal::Scope)
            }
        },
    );
    let mut node = node.exports(client, async |application| {
        Ok::<_, Never>(application.root().established_recipient())
    });
    let outcome = node.run().await.unwrap_or_else(|original| {
        drop(original);
        panic!("the supplied host executes the actual caller")
    });
    // This is a compile-only consumer. No native/runtime evidence is claimed.
    drop((outcome, node, results));
}

fn main() {
    let _construction = configured;
}
