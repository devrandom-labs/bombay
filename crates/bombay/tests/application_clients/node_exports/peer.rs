//! Private native peer. Application consumers never assemble this query loop.
use super::ProviderStage;
use super::protocol::ProtectedMessage;
use bombay::prelude::ShutdownRejection;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use zenoh::bytes::ZBytes;
use zenoh::qos::CongestionControl;
use zenoh::query::{ConsolidationMode, Reply};
use zenoh::{Config, Error as NativeError, Session};

pub(super) const COUNTER: &str = "bombay/node-consumer/counter";
pub(super) const LEDGER: &str = "bombay/node-consumer/ledger";

pub(super) struct Requests {
    pub(super) increment: ProtectedMessage,
    pub(super) conflict: ProtectedMessage,
    pub(super) read: ProtectedMessage,
    pub(super) stop: ProtectedMessage,
    pub(super) append: ProtectedMessage,
    pub(super) ledger_read: ProtectedMessage,
}

pub(super) struct Peer {
    session: Session,
}

pub(super) struct Observations {
    replies: Vec<Option<Reply>>,
    additional: Vec<Reply>,
    refused: Vec<Arc<Mutex<Option<TrySendError<Reply>>>>>,
    pub(super) closed: Result<(), NativeError>,
}

impl Observations {
    pub(super) fn received(&self) -> usize {
        self.replies.iter().flatten().count() + self.additional.len()
    }

    pub(super) fn protected(&self) -> Vec<ProtectedMessage> {
        assert!(self.additional.is_empty());
        let refused = self
            .refused
            .iter()
            .map(|original| original.lock().unwrap())
            .collect::<Vec<_>>();
        assert!(refused.iter().all(|original| original.is_none()));
        self.replies
            .iter()
            .map(|original| {
                let original = original.as_ref().expect(
                    "an actual publication precedes query closure; checked after Node cleanup",
                );
                let native = original.result();
                let sample = native
                    .as_ref()
                    .expect("actual native successful publication");
                ProtectedMessage {
                    protected: sample.payload().to_bytes().as_ref().into(),
                    proof: sample
                        .attachment()
                        .expect("actual provider proof attachment")
                        .to_bytes()
                        .as_ref()
                        .into(),
                }
            })
            .collect()
    }
}

impl Peer {
    pub(super) async fn quota(
        self,
        request: ProtectedMessage,
        shutdown: impl FnOnce() -> Result<(), ShutdownRejection>,
        retired: oneshot::Receiver<()>,
    ) -> (Observations, Result<(), ShutdownRejection>) {
        self.wait_for(COUNTER).await;
        let (mut first, first_refused) = self.query(COUNTER, &request).await;
        let mut replies = Vec::new();
        // Query FINAL must be observed while this actor and declaration remain live.
        // Preserve an unexpected reply as an observation and still finish cleanup.
        while let Some(original) = first.recv().await {
            replies.push(Some(original));
        }
        self.wait_for(COUNTER).await;
        let (mut second, second_refused) = self.query(COUNTER, &request).await;
        let mut additional = Vec::new();
        while let Some(original) = second.recv().await {
            additional.push(original);
        }
        self.wait_for(COUNTER).await;
        let shutdown = shutdown();
        retired
            .await
            .expect("full quota cleanup and Node close complete");
        let closed = self.session.close().await;
        (
            Observations {
                replies,
                additional,
                refused: vec![first_refused, second_refused],
                closed,
            },
            shutdown,
        )
    }

    pub(super) async fn close(self) -> Observations {
        let closed = self.session.close().await;
        Observations {
            replies: Vec::new(),
            additional: Vec::new(),
            refused: Vec::new(),
            closed,
        }
    }

    pub(super) async fn open(address: &str) -> Self {
        let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
        let config = Config::from_json5(&serde_json::json!({
            "mode":"peer", "connect":{"endpoints":[address]},
            "scouting":{"multicast":{"enabled":false,"listen":false},"gossip":{"enabled":false}},
            "transport":{"link":{"protocols":["tls"],"tls":{
                "root_ca_certificate":certificates.join("root-ca.pem"),
                "connect_private_key":certificates.join("caller-private-key.pem"),
                "connect_certificate":certificates.join("caller-certificate.pem"),
                "enable_mtls":true,"verify_name_on_connect":true,"use_public_pki":false
            }}}
        }).to_string()).expect("private finite native peer credentials");
        Self {
            session: zenoh::open(config).await.expect("actual peer opens"),
        }
    }

    pub(super) fn runtime_id(&self) -> String {
        self.session.zid().to_string()
    }

    async fn wait_for(&self, key: &str) {
        let querier = self
            .session
            .declare_querier(key)
            .await
            .expect("actual keyed querier");
        let (published, mut observed) = mpsc::channel(4);
        let refused = Arc::new(Mutex::new(None));
        let retained = Arc::clone(&refused);
        let listener = querier
            .matching_listener()
            .callback(move |status| {
                if let Err(original) = published.try_send(status) {
                    let mut retained = retained.lock().unwrap();
                    if retained.is_none() {
                        *retained = Some(original);
                    }
                }
            })
            .await
            .expect("causal native matching listener");
        let matched = loop {
            match observed.recv().await {
                Some(status) if status.matching() => break Some(status),
                Some(_) => {}
                None => break None,
            }
        };
        let withdrawn = listener.undeclare().await;
        let retired = querier.undeclare().await;
        assert!(withdrawn.is_ok() && retired.is_ok());
        assert!(matched.is_some());
        let refused = refused.lock().unwrap();
        assert!(refused.is_none());
    }

    async fn query(
        &self,
        key: &str,
        request: &ProtectedMessage,
    ) -> (
        mpsc::Receiver<Reply>,
        Arc<Mutex<Option<TrySendError<Reply>>>>,
    ) {
        let (published, receiving) = mpsc::channel(2);
        let refused = Arc::new(Mutex::new(None));
        let retained = Arc::clone(&refused);
        let submission = self
            .session
            .get(key)
            .payload(ZBytes::from(request.protected.as_ref()))
            .attachment(ZBytes::from(request.proof.as_ref()))
            .timeout(Duration::from_secs(20))
            .congestion_control(CongestionControl::Drop)
            .consolidation(ConsolidationMode::None)
            .callback(move |original| {
                if let Err(original) = published.try_send(original) {
                    let mut retained = retained.lock().unwrap();
                    if retained.is_none() {
                        *retained = Some(original);
                    }
                }
            })
            .await;
        submission.expect("actual finite native query submission");
        (receiving, refused)
    }

    #[expect(
        clippy::type_complexity,
        reason = "the sibling owns separate native request and causal oneshot originals"
    )]
    pub(super) async fn rejected(
        self,
        request: ProtectedMessage,
        sibling: Option<(
            ProtectedMessage,
            oneshot::Receiver<()>,
            oneshot::Receiver<()>,
            oneshot::Receiver<()>,
            oneshot::Sender<()>,
        )>,
        returned: oneshot::Receiver<()>,
        shutdown: impl AsyncFnOnce() -> Result<(), ShutdownRejection>,
        continued: oneshot::Sender<()>,
        retired: oneshot::Receiver<()>,
    ) -> (Observations, Result<(), ShutdownRejection>) {
        self.wait_for(COUNTER).await;
        let (mut receiving, refused) = self.query(COUNTER, &request).await;
        let mut sibling = match sibling {
            Some((request, entered, counter_pending, canceled, released)) => {
                counter_pending.await.expect(
                    "Counter verification was actually polled Pending before sibling progress",
                );
                self.wait_for(LEDGER).await;
                let receiving = self.query(LEDGER, &request).await;
                entered
                    .await
                    .expect("the sibling actually reached pending protection");
                canceled
                    .await
                    .expect("the borrowed execution wait was canceled with its loan held");
                let publication = released.send(());
                publication.expect(
                    "the Counter provider remains owned through borrowed-wait cancellation",
                );
                Some(receiving)
            }
            None => None,
        };
        returned
            .await
            .expect("the actual refusing producer returned its original cause");
        let shutdown = shutdown().await;
        let publication = continued.send(());
        publication.expect("invoked Work awaits the actual refusal and native read");
        retired
            .await
            .expect("the actual Node has completed cleanup and close");
        let mut replies = Vec::new();
        while let Some(original) = receiving.recv().await {
            replies.push(Some(original));
        }
        let mut refused = vec![refused];
        if let Some((receiving, rejected)) = &mut sibling {
            while let Some(original) = receiving.recv().await {
                replies.push(Some(original));
            }
            refused.push(Arc::clone(rejected));
        }
        let closed = self.session.close().await;
        (
            Observations {
                replies,
                additional: Vec::new(),
                refused,
                closed,
            },
            shutdown,
        )
    }

    pub(super) async fn exchange(
        self,
        requests: Requests,
        entered: oneshot::Receiver<()>,
        released: oneshot::Sender<()>,
        pending: ProviderStage,
        complete: impl FnOnce() -> Option<Result<(), ShutdownRejection>>,
        retired: oneshot::Receiver<()>,
    ) -> (Observations, Option<Result<(), ShutdownRejection>>) {
        self.wait_for(COUNTER).await;
        self.wait_for(LEDGER).await;
        let (mut increment, increment_refused) = self.query(COUNTER, &requests.increment).await;
        entered
            .await
            .expect("the Counter provider has actually reached its pending gate");
        let repeated = match pending {
            ProviderStage::Protection => Some((
                self.query(COUNTER, &requests.increment).await,
                self.query(COUNTER, &requests.conflict).await,
            )),
            ProviderStage::Verification => None,
        };
        let mut replies = Vec::new();
        let (mut appended, append_refused) = self.query(LEDGER, &requests.append).await;
        replies.push(appended.recv().await);
        let (mut ledger, ledger_refused) = self.query(LEDGER, &requests.ledger_read).await;
        replies.push(ledger.recv().await);
        let publication = released.send(());
        publication.expect("the actual pending Counter provider still owns its gate");
        replies.push(increment.recv().await);
        let ((mut duplicate, duplicate_refused), (mut conflict, conflict_refused)) = match repeated
        {
            Some(repeated) => repeated,
            None => (
                self.query(COUNTER, &requests.increment).await,
                self.query(COUNTER, &requests.conflict).await,
            ),
        };
        replies.push(duplicate.recv().await);
        let (mut read, read_refused) = self.query(COUNTER, &requests.read).await;
        replies.push(read.recv().await);
        let (mut stop, stop_refused) = self.query(COUNTER, &requests.stop).await;
        replies.push(stop.recv().await);
        let completion = complete();
        retired
            .await
            .expect("the actual Node has completed cleanup and close");
        let mut additional = Vec::new();
        for receiving in [
            &mut increment,
            &mut duplicate,
            &mut conflict,
            &mut appended,
            &mut ledger,
            &mut read,
            &mut stop,
        ] {
            while let Some(original) = receiving.recv().await {
                additional.push(original);
            }
        }
        let closed = self.session.close().await;
        (
            Observations {
                replies,
                additional,
                refused: vec![
                    increment_refused,
                    duplicate_refused,
                    conflict_refused,
                    append_refused,
                    ledger_refused,
                    read_refused,
                    stop_refused,
                ],
                closed,
            },
            completion,
        )
    }
}
