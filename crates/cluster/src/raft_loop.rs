//! `RaftLoop` — thin orchestrator over the two long-lived raft tasks
//! that share `Rc<RefCell<RaftNode>>` on a `LocalSet`:
//!
//! - [`crate::consensus::ConsensusLoop`] — drains inbound
//!   [`RequestVoteMsg`]s and drives the [`raft::Election`] borrow
//!   view.
//! - [`crate::replication::ReplicationLoop`] — drains inbound
//!   [`AppendEntriesMsg`]s, runs the periodic ledger watermark
//!   advance, and spawns per-peer outbound AE tasks. Both directions
//!   of replication live there.
//!
//! The two loops cooperate without mutexes because every `RefCell`
//! borrow is scoped to a single synchronous expression and the
//! `LocalSet` schedules them on one thread. The shared
//! [`crate::replication::ReplicationGate`] (clonable
//! `Arc<watch::Sender<bool>>`) carries every role transition so the
//! per-peer tasks pause / resume correctly regardless of which loop
//! observed the flip.

use crate::cluster_mirror::ClusterMirror;
use crate::command::{AppendEntriesMsg, RequestVoteMsg};
use crate::config::Config;
use crate::consensus::ConsensusLoop;
use crate::durable::DurablePersistence;
use crate::ledger_slot::LedgerSlot;
use crate::replication::{ReplicationGate, ReplicationLoop};
use raft::RaftNode;
use spdlog::{debug, info};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Bound on each inbound command channel. Picked to absorb a burst
/// of in-flight RPCs without blocking gRPC handlers, while still
/// giving backpressure under sustained overload. Same bound for AE
/// and RV — election should never get backlogged worse than AE.
pub(crate) const COMMAND_CHANNEL_DEPTH: usize = 1024;

pub struct RaftLoop {
    node: Rc<RefCell<RaftNode<DurablePersistence>>>,
    mirror: Arc<ClusterMirror>,
    config: Arc<Config>,
    ledger: Arc<LedgerSlot>,
    ae_rx: Option<mpsc::Receiver<AppendEntriesMsg>>,
    rv_rx: Option<mpsc::Receiver<RequestVoteMsg>>,
}

impl RaftLoop {
    /// Build a loop ready to be driven by [`RaftLoop::run`]. The
    /// caller owns both inbound channels: it creates `(ae_tx, ae_rx)`
    /// and `(rv_tx, rv_rx)`, hands the senders to the gRPC handlers,
    /// and passes the receivers here. The loop must be driven inside
    /// a `LocalSet` because it owns `Rc<RefCell<RaftNode>>` and is
    /// `!Send`.
    pub fn new(
        node: RaftNode<DurablePersistence>,
        ledger: Arc<LedgerSlot>,
        mirror: Arc<ClusterMirror>,
        config: Arc<Config>,
        ae_rx: mpsc::Receiver<AppendEntriesMsg>,
        rv_rx: mpsc::Receiver<RequestVoteMsg>,
    ) -> Self {
        Self {
            node: Rc::new(RefCell::new(node)),
            ledger,
            mirror,
            config,
            ae_rx: Some(ae_rx),
            rv_rx: Some(rv_rx),
        }
    }

    pub async fn run(mut self) {
        let self_id = self.node.borrow().self_id();
        debug!("raft_loop[{}]: starting consensus + replication", self_id);

        let (gate, gate_rx) = ReplicationGate::new();

        let consensus = ConsensusLoop::new(
            self.node.clone(),
            self.mirror.clone(),
            self.ledger.clone(),
            self.config.clone(),
            gate,
            self.rv_rx.take().expect("rv_rx initialized in new"),
            self_id,
        );
        let replication = ReplicationLoop::new(
            self.node.clone(),
            self.ledger.clone(),
            self.mirror.clone(),
            self.config.clone(),
            gate_rx,
            self.ae_rx.take().expect("ae_rx initialized in new"),
            self_id,
        );

        // `tokio::join!` runs both inline futures concurrently on the
        // current task. A panic in either propagates up through this
        // future to the caller's spawn_local — see `cluster::node`'s
        // `raft_local.await` handler, which re-raises any
        // `JoinError::is_panic()`.
        tokio::join!(consensus.run(), replication.run());

        debug!("raft_loop[{}]: both loops exited", self_id);
    }
}
