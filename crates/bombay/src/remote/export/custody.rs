//! Private sole-custody law; this does not claim native completion or Node cleanup.
use super::super::request::{Custody, Identity, Request};
use super::super::{Attempt, Export, ExportLimits, RefusalRetention, submission};
use crate::remote::json;
use crate::{MailAddr, TrySendError};
use behavior::MessageProtocol;
use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::rc::Rc;

struct Refusal<'scope>(&'scope Cell<usize>);

impl Drop for Refusal<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[test]
fn discharge_preserves_a_request_until_its_last_producer_ends() {
    let disposed = Cell::new(0);
    let maximum = NonZeroUsize::new(1).unwrap();
    let export = Export::new(
        "custody/refusal",
        ExportLimits {
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
            refusals: RefusalRetention::Discharge,
        },
        json::Limits {
            input_bytes: maximum,
            proof_bytes: maximum,
            token_bytes: maximum,
            nesting: maximum,
            parser_states: maximum,
        },
        |_: &[u8]| Ok::<_, ()>(7_u8),
        submission::<_, MessageProtocol<MailAddr, u8>, _, _>(
            |message: u8| Ok::<_, u8>(move || message),
            |(): &(), _: &Attempt<Result<(), TrySendError<u8>>>| {
                Ok::<_, ()>(Box::<[u8]>::from([1_u8]))
            },
        ),
        async |_: &[u8], _: &[u8]| Err::<(), _>(Refusal(&disposed)),
        async |_: &[u8], (): &()| Ok::<_, ()>(Box::<[u8]>::from([2_u8])),
        |(): &(), (): &()| true,
        |(): &(), _: &str, _: MailAddr| Ok::<_, ()>(()),
        |(): &(), _: &str, _: MailAddr| Ok::<_, ()>(()),
    );
    // Deliberate private-owner state, not fabricated network acceptance evidence.
    let request = Request {
        custody: Rc::new(Custody {
            identity: RefCell::new(Identity::Unassigned),
            original: (Box::from([3_u8]), Box::from([5_u8])),
            query: RefCell::new(None),
            verified: RefCell::new(Some(Err(Refusal(&disposed)))),
            decoded: RefCell::new(None),
            native: RefCell::new(None),
            encoding: RefCell::new(None),
            protection: RefCell::new(None),
            publications: RefCell::new(Vec::new()),
            request_refusal: RefCell::new(None),
            reply_refusal: RefCell::new(None),
            refusal: RefCell::new(None),
            verification_factory: RefCell::new(None),
            verification_poll: RefCell::new(None),
            verification_disposal: RefCell::new(None),
            protection_factory: RefCell::new(None),
            protection_poll: RefCell::new(None),
            protection_disposal: RefCell::new(None),
            completed: RefCell::new(Some(())),
        }),
    };
    let producer = Rc::clone(&request.custody);
    let released = Rc::downgrade(&producer);
    export.entries.borrow_mut().push(Some(request));
    export.discharge();
    let first = {
        let entries = export.entries.borrow();
        entries[0]
            .as_ref()
            .map(|request| Rc::ptr_eq(&request.custody, &producer))
    };
    let before = disposed.get();
    drop(producer);
    export.discharge();
    let after = disposed.get();
    let vacant = export.entries.borrow()[0].is_none();
    let remaining = released.strong_count();
    drop(export);
    assert_eq!(
        first,
        Some(true),
        "a live producer keeps its exact original request occupied"
    );
    assert_eq!((before, after, disposed.get()), (0, 1, 1));
    assert!(vacant && remaining == 0);
}
