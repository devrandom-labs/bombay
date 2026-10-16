//! Native absence and a supplied unit interpreter are distinct capabilities.
use bombay::behavior::{
    ActionItem, Actions, BehaviorActed, InterpretItem, InterpreterRequests, ItemSettlement, Never,
    SettledItem, Step,
};
use bombay::{ActorRetirement, Application, SettlementFailure, WorkOutcome};
use core::future::Future;
use std::io::{self, Read};
use std::path::PathBuf;

#[path = "application_services/requests.rs"]
mod requests;
use requests::{FileAccess, OpenFile, Services};

impl bombay::ExternalRequest for OpenFile {
    type Interpreter = ();
}

impl<Event, Path> InterpretItem<OpenFile, Event, Path> for () {
    #[expect(
        clippy::manual_async_fn,
        reason = "the receiving loan captures no event or route lifetime"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <OpenFile as ActionItem>::Input<'a>,
        received: &'a mut Option<<OpenFile as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        OpenFile: 'a,
    {
        async move {
            let mut files = Services;
            <Services as InterpretItem<OpenFile, Event, Path>>::interpret_item(
                &mut files, input, received,
            )
            .await;
        }
    }
}

struct Document {
    files: Vec<OpenFile>,
    original: Box<[u8; 2]>,
}

#[bombay::actor(message = Never, sends = { files: InterpreterRequests<OpenFile> })]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the actor returns its complete typed Actions through the standard error carrier"
)]
impl Document {
    fn init(&mut self) -> BehaviorActed<Self> {
        let mut actions = Actions::stop();
        for file in self.files.drain(..) {
            actions = actions.with_send(file);
        }
        Ok(actions)
    }
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one native unit interpreter journey preserves every independent phase, receipt, original rejection and retirement lane"
)]
async fn explicit_unit_interpreter_performs_real_io_and_retains_complete_rejection() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let rejected = path.join("missing");
    let rejected_pointer = rejected.as_os_str().as_encoded_bytes().as_ptr();
    let expected_rejected = rejected.clone();
    let original = Box::new([97, 101]);
    let actor_pointer = original.as_ptr();
    let (work, native, notifications, ()) = Application::new(Document {
        files: vec![
            OpenFile {
                path,
                access: FileAccess::Ready,
            },
            OpenFile {
                path: rejected,
                access: FileAccess::Ready,
            },
        ],
        original,
    })
    .with_interpreter(|| ())
    .run()
    .await
    .unwrap_or_else(|_| panic!("the native host is entered"))
    .into_parts();
    let WorkOutcome::NotInvoked {
        work: None,
        startup_error: Some(_),
        cleanup,
    } = work
    else {
        panic!("initialization stop cannot publish application readiness");
    };
    cleanup.expect("the supplied unit interpreter retires with actual results");
    let notifications = notifications.expect("independent actual notification results");
    notifications
        .termination
        .expect("actual termination publication");
    notifications
        .retirement
        .expect("actual retirement publication");
    let (_, retirement) = native.expect("the original actor remains independently owned");
    let ActorRetirement::EffectsFailed {
        custody,
        initialization,
        error,
        settlements,
        interpretation,
        source,
    } = retirement
    else {
        panic!("initialization rejection preserves both real outcomes and rejects readiness");
    };
    assert_eq!(error, SettlementFailure::Rejected);
    assert!(initialization.is_none());
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(custody.behavior.original.as_ptr(), actor_pointer);
    assert_eq!(&*custody.behavior.original, &[97, 101]);
    assert_eq!(custody.behavior.files.len(), 0);
    assert_eq!(custody.control.len(), 0);
    assert_eq!(custody.user.len(), 0);
    assert_eq!(custody.descendants.len(), 0);
    assert_eq!(custody.child_failures.len(), 0);
    assert_eq!(custody.capability_failures.len(), 0);
    assert_eq!(custody.additional_failures.len(), 0);
    assert_eq!(custody.retirement_failures.len(), 0);
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
    let mut settlements = settlements.into_iter();
    let settled = settlements
        .next()
        .expect("the complete initialized file lane");
    assert!(settlements.next().is_none());
    assert_eq!(settled.creations.len(), 0);
    assert!(matches!(settled.become_, Step::Stop(_)));
    let mut files = settled.sends.files.into_iter();
    let Some(SettledItem::Attempted(ItemSettlement::Accepted(mut file))) = files.next() else {
        panic!("the supplied unit interpreter opens the actual original file");
    };
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("the acquired file remains usable after retirement");
    assert_eq!(contents, include_str!("../Cargo.toml"));
    let Some(SettledItem::Attempted(ItemSettlement::Rejected { item, reason })) = files.next()
    else {
        panic!(
            "the complete original request and actual OS cause coexist with the accepted prefix"
        );
    };
    assert!(files.next().is_none());
    assert_eq!(
        item.path.as_os_str().as_encoded_bytes().as_ptr(),
        rejected_pointer
    );
    assert_eq!(item.path, expected_rejected);
    assert!(matches!(item.access, FileAccess::Ready));
    assert_eq!(reason.kind(), io::ErrorKind::NotADirectory);
}

#[test]
fn native_interpreter_rejects_absence_and_wrong_owner() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/interpreter_missing.rs");
    cases.compile_fail("tests/compile/fail/interpreter_owner.rs");
}
