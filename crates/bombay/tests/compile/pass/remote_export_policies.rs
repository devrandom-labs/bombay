use bombay::actors::ActorExt;
use bombay::behavior::Never;
use bombay::remote::{Export, ExportLimits, Network, json, reply, stop};
use bombay::{Application, MailAddr};

#[path = "../../../../../examples/counter/src/counter.rs"]
#[expect(
    dead_code,
    reason = "the fixture reuses actual nominal Counter operations"
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
    reason = "the fixture reuses the actual typed operation declarations and wire functions"
)]
mod protocol;

use deployment::{Deployment, Purpose, Receipt};

async fn configured(
    network: Network,
    configured: &Deployment,
    counts: ExportLimits,
    framing: json::Limits,
) {
    let node = Application::new(counter::Counter::new().stop_on_shutdown())
        .open_node(network)
        .await
        .unwrap_or_else(|original| {
            drop(original);
            panic!("the supplied native configuration opens")
        });
    let current = |receipt: &Receipt, runtime: &str, target: MailAddr| {
        let grant = configured.metadata(receipt.operation())?;
        if receipt.metadata() == grant
            && grant.runtime_generation.as_ref() == runtime
            && grant.actor_generation == target.0
        {
            Ok(())
        } else {
            Err(deployment::Refusal::Scope)
        }
    };
    let export = Export::new(
        protocol::KEY,
        counts,
        framing,
        |bytes: &[u8]| serde_json::from_slice::<protocol::Request>(bytes),
        (
            reply(protocol::increment, protocol::processed),
            (
                reply(protocol::read, protocol::value),
                stop(protocol::stop, protocol::stopped),
            ),
        ),
        async |bytes: &[u8], proof: &[u8]| configured.verify(Purpose::Request, bytes, proof),
        async |bytes: &[u8], receipt: &Receipt| {
            let parsed: protocol::Response = serde_json::from_slice(bytes)
                .map_err(|cause| deployment::Refusal::Json { cause })?;
            if parsed.bombay != *receipt.metadata()
                || parsed.operation != receipt.operation()
                || parsed.kind != Purpose::Reply
            {
                return Err(deployment::Refusal::Scope);
            }
            configured.protect(Purpose::Reply, bytes)
        },
        |left: &Receipt, right: &Receipt| {
            left.operation() == right.operation() && left.metadata() == right.metadata()
        },
        &current,
        &current,
    );
    let mut node = node.exports(export, async |application| {
        Ok::<_, Never>(application.root().established_recipient())
    });
    let outcome = node.run().await.unwrap_or_else(|original| {
        drop(original);
        panic!("the supplied host executes the actual Counter")
    });
    // This is a compile-only consumer. No native/runtime evidence is claimed.
    drop((outcome, node));
}

fn main() {
    let _construction = configured;
}
