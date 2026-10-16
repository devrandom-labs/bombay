use bombay::ActorCustody;
use bombay::behavior::{
    BehaviorSettlements, ClassifySettlement, Never, Protocol, SettlementStatus,
};
use bombay::prelude::{ActorRetirement, Completion, MailAddr, RootOrigin};

// Actual native custody, not a reduced shadow outcome. Every native field is inspected.
pub(super) fn completed_actor<B>(
    origin: RootOrigin<B>,
    retirement: ActorRetirement<B, Never, Vec<Never>>,
) -> B
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    B::Settlements: ClassifySettlement,
{
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                behavior,
                control,
                user,
                descendants,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
            },
        settlements,
        completion,
        interpretation,
        source,
    } = retirement
    else {
        panic!("actual native actor failed; no clean result is synthesized");
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(child_failures.as_slice(), []);
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    let assessment = settlements.settlement_status();
    assert_eq!(assessment, SettlementStatus::Accepted);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert_eq!(descendants.as_slice(), [] as [Never; 0]);
    assert_eq!(completion, Completion::Stopped);
    behavior
}
