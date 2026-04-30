//! Raft driver — the cluster-side I/O wrapper around the pure-state-machine
//! `raft` crate (ADR-0017). Owns the single `tokio::sync::Mutex<RaftNode>`
//! that serialises every node-facing RPC, plus the per-peer outbound RPC
//! channels, the wakeup sleeper, and the ledger reseed path.
//!
//! Concurrency model (per the user's directive on this migration):
//!
//! - All `node.proto` RPC handlers run sequentially through one
//!   `Arc<TokioMutex<RaftNode<DurablePersistence>>>`. There is no actor
//!   loop / mpsc channel — every entry locks the mutex itself.
//! - `LedgerHandler` (client gRPC) reads through [`ClusterMirror`], not
//!   through the raft mutex, so writes are never blocked by raft state
//!   transitions.
//! - Outbound RPCs (`SendAppendEntries`, `SendRequestVote`) are dispatched
//!   as detached tokio tasks. Each task fires the RPC, on reply re-locks
//!   the raft mutex via [`RaftHandle::on_outbound_reply`]. The wakeup
//!   sleeper and the on-commit hook also lock when they need to step.
//!
//! Per ADR-0017 §"What raft Owns" the library never reads a clock; the
//! driver supplies `Instant::now()` to every `step()` call.

use crate::cluster_mirror::ClusterMirror;
use crate::config::{Config, PeerConfig};
use crate::durable::DurablePersistence;
use crate::ledger_slot::LedgerSlot;
use ::proto::node as proto;
use ::proto::node::node_client::NodeClient;
use ledger::ledger::Ledger;
use raft::{Action, Event, LogEntryRange, RaftConfig, RaftNode, RejectReason};
use spdlog::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use storage::wal_tail::WAL_RECORD_SIZE;
use tokio::sync::{Mutex as TokioMutex, RwLock as TokioRwLock};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint};

// ── Public handle ─────────────────────────────────────────────────────────

/// I/O wrapper around `RaftNode`. One per cluster node, shared via `Arc`.
pub struct RaftHandle {
    /// Single point of mutation for raft state. All `step()` calls hold
    /// this mutex; the driver does not maintain a parallel "in-flight"
    /// view.
    raft: TokioMutex<RaftNode<DurablePersistence>>,
    /// Lock-free read surface for `LedgerHandler` and other clients.
    pub mirror: Arc<ClusterMirror>,
    /// Durable term + vote log. Held here so `LedgerHandler` can read
    /// `Term::get_term_at_tx` outside the raft mutex.
    pub durable: Arc<DurablePersistence>,
    /// Indirection to the live `Arc<Ledger>`. Reseed swaps the inner
    /// pointer atomically; long-lived holders see the new ledger on the
    /// next `slot.ledger()` call.
    pub ledger: Arc<LedgerSlot>,
    /// Cluster configuration (peer list, replication knobs).
    pub config: Arc<Config>,
    /// Per-peer reusable gRPC channels (excluding self). Lazily refreshed
    /// on transport failure.
    peers: TokioMutex<PeerChannels>,
    /// Single wakeup sleeper task; re-armed on every `Action::SetWakeup`.
    wakeup: WakeupTask,
    self_id: u64,
    /// Flipped on `Action::FatalError`. Idempotent.
    fatal: AtomicBool,
    /// Flipped on supervisor drop to silence detached subtasks.
    drained: AtomicBool,
    /// Pending local-commit watermark fed by `Ledger::on_commit`. The
    /// hook itself runs on the ledger's pipeline thread (not tokio) and
    /// must remain a single atomic instruction — no `tokio::spawn`, no
    /// `await`. The value is drained synchronously inside every step
    /// entry point (handle_append_entries / handle_request_vote /
    /// on_tick / on_outbound_reply) before processing the real event.
    /// Only the leader interprets it as `Event::LocalCommitAdvanced`;
    /// non-leaders swap-to-zero to discard.
    pending_local_commit: Arc<AtomicU64>,
    /// Minimum cadence at which the wakeup sleeper is re-armed when
    /// raft has no pending deadline of its own. Single-node leaders
    /// emit no `SetWakeup` action (no peers to heartbeat, election
    /// timer disarmed), so without this fallback the `on_commit` hook
    /// stream never gets drained. Set from `RaftConfig::heartbeat_interval`.
    fallback_wakeup_interval: std::time::Duration,
}

impl RaftHandle {
    /// Build the driver. Loads durable persistence, constructs the raft
    /// node, opens per-peer channels lazily on first dispatch.
    pub fn open(
        config: Arc<Config>,
        ledger: Arc<LedgerSlot>,
        durable: Arc<DurablePersistence>,
        mirror: Arc<ClusterMirror>,
    ) -> std::io::Result<Arc<Self>> {
        let cluster = config
            .cluster
            .as_ref()
            .ok_or_else(|| std::io::Error::other("RaftHandle requires a clustered config"))?;
        let self_id = cluster.node.node_id;
        let peer_ids: Vec<u64> = cluster.peers.iter().map(|p| p.peer_id).collect();

        let raft_cfg = RaftConfig::default();
        let fallback_wakeup_interval = raft_cfg.heartbeat_interval;
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
            ^ self_id;

        let persistence_for_node = DurablePersistence {
            term: durable.term.clone(),
            vote: durable.vote.clone(),
        };
        let mut node =
            RaftNode::new(self_id, peer_ids, persistence_for_node, raft_cfg, seed);
        // ADR-0017 has raft track `local_log_index` only via
        // `Event::LogAppendComplete`. On restart the ledger may already
        // hold tx 1..N persistently — without an explicit hint, raft
        // starts at `local_log_index = 0` and §5.4.1's up-to-date check
        // would let an out-of-date candidate win an election. Inform
        // raft of the durable log size before it accepts any inbound
        // event.
        let durable_last_tx_id = ledger.ledger().last_commit_id();
        if durable_last_tx_id > 0 {
            let _ = node.step(
                Instant::now(),
                Event::LogAppendComplete {
                    tx_id: durable_last_tx_id,
                },
            );
        }
        // Snapshot initial state so consumers see post-construction values.
        mirror.snapshot_from(&node);

        let handle = Arc::new(Self {
            raft: TokioMutex::new(node),
            mirror,
            durable,
            ledger,
            config: config.clone(),
            peers: TokioMutex::new(PeerChannels::new(cluster.peers.clone(), self_id)),
            wakeup: WakeupTask::new(),
            self_id,
            fatal: AtomicBool::new(false),
            drained: AtomicBool::new(false),
            pending_local_commit: Arc::new(AtomicU64::new(0)),
            fallback_wakeup_interval,
        });

        // Arm the initial wakeup so the election timer starts running.
        // First step is deferred to the supervisor's caller (which fires
        // the kickoff `on_tick().await` after construction).
        Ok(handle)
    }

    /// Drain the `pending_local_commit` atomic into raft (or discard
    /// it if not currently leader). Called under the raft mutex at the
    /// top of every step entry point. Cheap when there's nothing
    /// pending — single `swap` returns 0 and we return early.
    fn drain_pending_local_commit_locked(
        self: &Arc<Self>,
        node: &mut RaftNode<DurablePersistence>,
    ) -> StepOutcome {
        let pending = self.pending_local_commit.swap(0, Ordering::AcqRel);
        if pending == 0 {
            return StepOutcome::default();
        }
        if !matches!(node.role(), raft::Role::Leader) {
            trace!(
                "raft_driver[{}]: drain skipped (role={:?}, pending={})",
                self.self_id,
                node.role(),
                pending
            );
            return StepOutcome::default();
        }
        trace!(
            "raft_driver[{}]: drain feeding LocalCommitAdvanced(tx_id={})",
            self.self_id, pending
        );
        process_step(
            node,
            &self.mirror,
            Instant::now(),
            Event::LocalCommitAdvanced { tx_id: pending },
        )
    }

    /// Inbound `AppendEntries` flow. Holds the raft mutex for the entire
    /// RPC duration — including the ledger write — so all `node.proto`
    /// handlers are strictly sequential.
    pub async fn handle_append_entries(
        self: &Arc<Self>,
        req: proto::AppendEntriesRequest,
    ) -> proto::AppendEntriesResponse {
        let from = req.leader_id;
        let term = req.term;
        let prev_log_tx_id = req.prev_tx_id;
        let prev_log_term = req.prev_term;
        let leader_commit = req.leader_commit_tx_id;
        let payload = storage::wal_tail::decode_records(&req.wal_bytes);
        let count = if req.to_tx_id >= req.from_tx_id && !payload.is_empty() {
            (req.to_tx_id - req.from_tx_id + 1) as u64
        } else {
            0
        };
        let entries = LogEntryRange {
            start_tx_id: req.from_tx_id,
            count,
            term,
        };

        let initial = Event::AppendEntriesRequest {
            from,
            term,
            prev_log_tx_id,
            prev_log_term,
            entries,
            leader_commit,
        };

        let mut node = self.raft.lock().await;
        // Drain any pending local commit before the real event so the
        // leader's match_index reflects the latest durable progress.
        let drain = self.drain_pending_local_commit_locked(&mut node);
        self.dispatch_outbound_and_wakeup(&drain);
        let mut current_event = initial;
        loop {
            let outcome = process_step(&mut node, &self.mirror, Instant::now(), current_event);
            self.dispatch_outbound_and_wakeup(&outcome);
            if outcome.fatal.is_some() {
                self.fatal.store(true, Ordering::Release);
            }
            if let Some(after_tx) = outcome.truncate_log {
                self.do_reseed(after_tx).await;
                current_event = Event::LogTruncateComplete { up_to: after_tx };
                continue;
            }
            if let Some(range) = outcome.append_log {
                let entries = payload.clone();
                let ledger = self.ledger.ledger();
                let last = range.last_tx_id().expect("non-empty range");
                // ADR-0017 §"Durability before externalisation" #5b:
                // the follower's success reply must wait for actual
                // durability. `append_wal_entries` only enqueues —
                // we then block until `last_commit_id` reaches `last`.
                // The user's "all node.proto handlers must be
                // sequential" directive keeps the raft mutex held
                // during this wait.
                let res = tokio::task::spawn_blocking(move || {
                    ledger.append_wal_entries(entries)?;
                    while ledger.last_commit_id() < last {
                        std::thread::sleep(std::time::Duration::from_micros(50));
                    }
                    Ok::<(), std::io::Error>(())
                })
                .await
                .unwrap_or_else(|e| {
                    Err(std::io::Error::other(format!(
                        "append_wal_entries spawn_blocking panicked: {}",
                        e
                    )))
                });
                if let Err(e) = res {
                    warn!(
                        "raft_driver: append_wal_entries failed on follower path: {}",
                        e
                    );
                    return proto::AppendEntriesResponse {
                        term: self.mirror.current_term(),
                        success: false,
                        last_tx_id: self.ledger.ledger().last_commit_id(),
                        reject_reason: proto::RejectReason::RejectWalAppendFailed as u32,
                    };
                }
                current_event = Event::LogAppendComplete { tx_id: last };
                continue;
            }
            // Reply emitted — extract and return.
            let resp = match outcome.ae_reply {
                Some(action) => action_to_ae_response(action),
                None => proto::AppendEntriesResponse {
                    term: self.mirror.current_term(),
                    success: false,
                    last_tx_id: self.ledger.ledger().last_commit_id(),
                    reject_reason: proto::RejectReason::RejectNotFollower as u32,
                },
            };
            drop(node);
            return resp;
        }
    }

    /// Inbound `RequestVote` flow. Single step, never emits AppendLog.
    pub async fn handle_request_vote(
        self: &Arc<Self>,
        req: proto::RequestVoteRequest,
    ) -> proto::RequestVoteResponse {
        let event = Event::RequestVoteRequest {
            from: req.candidate_id,
            term: req.term,
            last_tx_id: req.last_tx_id,
            last_term: req.last_term,
        };
        let mut node = self.raft.lock().await;
        let drain = self.drain_pending_local_commit_locked(&mut node);
        self.dispatch_outbound_and_wakeup(&drain);
        let outcome = process_step(&mut node, &self.mirror, Instant::now(), event);
        self.dispatch_outbound_and_wakeup(&outcome);
        if outcome.fatal.is_some() {
            self.fatal.store(true, Ordering::Release);
        }
        let resp = match outcome.rv_reply {
            Some(action) => action_to_rv_response(action),
            None => proto::RequestVoteResponse {
                term: self.mirror.current_term(),
                vote_granted: false,
            },
        };
        drop(node);
        resp
    }

    /// Wakeup-fired tick. Drains any pending local commit, then steps
    /// `Event::Tick`. The wakeup task self-cancels on drop, so extra
    /// `on_tick` calls after teardown are harmless.
    pub async fn on_tick(self: &Arc<Self>) {
        let mut node = self.raft.lock().await;
        let drain = self.drain_pending_local_commit_locked(&mut node);
        self.dispatch_outbound_and_wakeup(&drain);
        let outcome = process_step(&mut node, &self.mirror, Instant::now(), Event::Tick);
        self.dispatch_outbound_and_wakeup(&outcome);
        if outcome.fatal.is_some() {
            self.fatal.store(true, Ordering::Release);
        }
        drop(node);
    }

    /// Re-enter the raft state machine with a reply event from a
    /// spawned outbound RPC task. Same shape as `on_tick`.
    async fn on_outbound_reply(self: &Arc<Self>, ev: Event) {
        if self.drained.load(Ordering::Acquire) {
            return;
        }
        let mut node = self.raft.lock().await;
        let drain = self.drain_pending_local_commit_locked(&mut node);
        self.dispatch_outbound_and_wakeup(&drain);
        let outcome = process_step(&mut node, &self.mirror, Instant::now(), ev);
        self.dispatch_outbound_and_wakeup(&outcome);
        if outcome.fatal.is_some() {
            self.fatal.store(true, Ordering::Release);
        }
        drop(node);
    }

    /// Lock-free accessor for the on-commit hook closure. Returns the
    /// shared atomic that the closure mutates with a single
    /// `fetch_max`. Drained synchronously inside every step entry
    /// point.
    pub fn pending_local_commit(&self) -> Arc<AtomicU64> {
        self.pending_local_commit.clone()
    }

    /// Stop spawning new detached subtasks. Called by the supervisor's
    /// drop path after the gRPC servers have been notified to stop
    /// accepting new requests, so further reply events are pointless.
    /// Idempotent.
    pub fn drain(&self) {
        self.drained.store(true, Ordering::Release);
        self.wakeup.cancel();
    }

    #[inline]
    pub fn is_fatal(&self) -> bool {
        self.fatal.load(Ordering::Acquire)
    }

    #[inline]
    pub fn self_id(&self) -> u64 {
        self.self_id
    }

    /// Dispatch outbound side-effects collected from a step. Spawns
    /// detached tasks for outbound RPCs (so the raft mutex doesn't
    /// block on slow peers) and reschedules the wakeup sleeper.
    ///
    /// If raft did not request a wakeup (e.g. single-node leader with
    /// no peers and a disarmed election timer), schedule a fallback at
    /// `fallback_wakeup_interval`. This keeps the Tick loop alive so
    /// `pending_local_commit` updates from `Ledger::on_commit` are
    /// drained and `cluster_commit_index` advances.
    fn dispatch_outbound_and_wakeup(self: &Arc<Self>, outcome: &StepOutcome) {
        if self.drained.load(Ordering::Relaxed) {
            return;
        }
        for action in &outcome.outbound_ae {
            self.spawn_outbound_append_entries(action.clone());
        }
        for action in &outcome.outbound_rv {
            self.spawn_outbound_request_vote(action.clone());
        }
        let at = outcome
            .wakeup
            .unwrap_or_else(|| Instant::now() + self.fallback_wakeup_interval);
        self.wakeup.reschedule(at, Arc::downgrade(self));
    }

    fn spawn_outbound_append_entries(self: &Arc<Self>, action: Action) {
        let Action::SendAppendEntries {
            to,
            term,
            prev_log_tx_id,
            prev_log_term,
            entries,
            leader_commit,
        } = action
        else {
            return;
        };

        let weak = Arc::downgrade(self);
        let leader_id = self.self_id;
        let max_bytes = self.config.cluster.as_ref().map(|c| c.append_entries_max_bytes).unwrap_or(4 * 1024 * 1024);
        let rpc_msg_limit = max_bytes * 2 + 4 * 1024;
        let ledger = self.ledger.ledger();

        tokio::spawn(async move {
            // Read payload bytes from the WAL covering the requested
            // range. A single tx_id can span multiple 40-byte WAL
            // records (Metadata + Entry/Link), so we cannot bound the
            // shipment by record count — read up to `max_bytes` and
            // trim to records with `tx_id <= start_tx_id + count - 1`
            // so raft's per-peer next_index isn't overshot. The actual
            // last tx_id in the shipment is reported on the wire as
            // `to_tx_id`; raft sees that on the success reply via the
            // follower's `last_tx_id` and advances `match_index`
            // accordingly (partial fulfillment is fine — any unshipped
            // tx_ids fall into the next AppendEntries pass).
            let wal_bytes = if entries.count == 0 {
                Vec::new()
            } else {
                let last_tx_id =
                    entries.last_tx_id().expect("non-empty range has last_tx_id");
                read_wal_range(&ledger, entries.start_tx_id, last_tx_id, max_bytes)
            };

            // Establish or reuse the channel.
            let Some(handle) = weak.upgrade() else {
                return;
            };
            let mut peers = handle.peers.lock().await;
            let client_res = peers.get_or_connect(to, rpc_msg_limit).await;
            // Snapshot client + drop the peers lock before doing the actual RPC,
            // so other outbound dispatches don't queue behind us.
            let mut client = match client_res {
                Ok(c) => c.clone(),
                Err(e) => {
                    debug!(
                        "raft_driver: outbound AE connect to {} failed: {} (treating as RPC timeout)",
                        to, e
                    );
                    drop(peers);
                    drop(handle);
                    if let Some(h) = weak.upgrade() {
                        h.on_outbound_reply(Event::AppendEntriesReply {
                            from: to,
                            term,
                            success: false,
                            last_tx_id: 0,
                            reject_reason: Some(RejectReason::RpcTimeout),
                        })
                        .await;
                    }
                    return;
                }
            };
            drop(peers);
            drop(handle);

            // Use the actual last record's tx_id when we have payload —
            // it may differ from `entries.last_tx_id()` when the byte
            // budget cut the shipment short. For an empty heartbeat,
            // fall back to `prev_log_tx_id` (the legacy convention).
            let to_tx_id = tx_id_of_last_record(&wal_bytes)
                .unwrap_or_else(|| entries.last_tx_id().unwrap_or(prev_log_tx_id));
            let req = proto::AppendEntriesRequest {
                leader_id,
                term,
                prev_tx_id: prev_log_tx_id,
                prev_term: prev_log_term,
                from_tx_id: entries.start_tx_id,
                to_tx_id,
                wal_bytes,
                leader_commit_tx_id: leader_commit,
            };

            let result = client.append_entries(req).await;
            let ev = match result {
                Ok(resp) => {
                    let r = resp.into_inner();
                    Event::AppendEntriesReply {
                        from: to,
                        term: r.term,
                        success: r.success,
                        last_tx_id: r.last_tx_id,
                        reject_reason: if r.success {
                            None
                        } else {
                            map_proto_reject(r.reject_reason)
                        },
                    }
                }
                Err(e) => {
                    trace!(
                        "raft_driver: outbound AE to {} transport error: {} ({})",
                        to,
                        e.code(),
                        e.message()
                    );
                    // Mark the channel as dead so the next dispatch
                    // re-establishes.
                    if let Some(h) = weak.upgrade() {
                        let mut peers = h.peers.lock().await;
                        peers.invalidate(to);
                    }
                    Event::AppendEntriesReply {
                        from: to,
                        term,
                        success: false,
                        last_tx_id: 0,
                        reject_reason: Some(RejectReason::RpcTimeout),
                    }
                }
            };

            if let Some(h) = weak.upgrade() {
                h.on_outbound_reply(ev).await;
            }
        });
    }

    fn spawn_outbound_request_vote(self: &Arc<Self>, action: Action) {
        let Action::SendRequestVote {
            to,
            term,
            last_tx_id,
            last_term,
        } = action
        else {
            return;
        };
        let weak = Arc::downgrade(self);
        let candidate_id = self.self_id;
        let max_bytes = self
            .config
            .cluster
            .as_ref()
            .map(|c| c.append_entries_max_bytes)
            .unwrap_or(4 * 1024 * 1024);
        let rpc_msg_limit = max_bytes * 2 + 4 * 1024;

        tokio::spawn(async move {
            let Some(handle) = weak.upgrade() else {
                return;
            };
            let mut peers = handle.peers.lock().await;
            let client_res = peers.get_or_connect(to, rpc_msg_limit).await;
            let mut client = match client_res {
                Ok(c) => c.clone(),
                Err(e) => {
                    debug!(
                        "raft_driver: outbound RV connect to {} failed: {} (treating as RPC timeout)",
                        to, e
                    );
                    drop(peers);
                    drop(handle);
                    if let Some(h) = weak.upgrade() {
                        h.on_outbound_reply(Event::RequestVoteReply {
                            from: to,
                            term,
                            granted: false,
                        })
                        .await;
                    }
                    return;
                }
            };
            drop(peers);
            drop(handle);

            let req = proto::RequestVoteRequest {
                term,
                candidate_id,
                last_tx_id,
                last_term,
            };
            let result = client.request_vote(req).await;
            let ev = match result {
                Ok(resp) => {
                    let r = resp.into_inner();
                    Event::RequestVoteReply {
                        from: to,
                        term: r.term,
                        granted: r.vote_granted,
                    }
                }
                Err(e) => {
                    trace!(
                        "raft_driver: outbound RV to {} transport error: {} ({})",
                        to,
                        e.code(),
                        e.message()
                    );
                    if let Some(h) = weak.upgrade() {
                        let mut peers = h.peers.lock().await;
                        peers.invalidate(to);
                    }
                    Event::RequestVoteReply {
                        from: to,
                        term,
                        granted: false,
                    }
                }
            };

            if let Some(h) = weak.upgrade() {
                h.on_outbound_reply(ev).await;
            }
        });
    }

    /// Reseed the ledger to the divergence watermark, then atomically
    /// install the new ledger and re-register the on-commit hook.
    /// Called under the raft mutex so subsequent steps see the new
    /// ledger without any window of inconsistency.
    async fn do_reseed(self: &Arc<Self>, after_tx_id: u64) {
        info!(
            "raft_driver: reseed BEGIN node_id={} after_tx_id={}",
            self.self_id, after_tx_id
        );
        let cfg = self.config.ledger.clone();
        let res = tokio::task::spawn_blocking(move || -> std::io::Result<Arc<Ledger>> {
            let mut ledger = Ledger::new(cfg);
            ledger.start_with_recovery_until(after_tx_id)?;
            Ok(Arc::new(ledger))
        })
        .await;
        let new_ledger = match res {
            Ok(Ok(l)) => l,
            Ok(Err(e)) => {
                error!("raft_driver: reseed failed: {}", e);
                return;
            }
            Err(e) => {
                error!("raft_driver: reseed spawn_blocking panicked: {}", e);
                return;
            }
        };

        // Re-register the on-commit hook on the new ledger so the local
        // commit stream keeps flowing into raft via the pending atomic.
        register_on_commit_hook(&new_ledger, self.pending_local_commit.clone());

        let _old = self.ledger.replace(new_ledger.clone());
        info!(
            "raft_driver: reseed END node_id={} new_last_commit_id={}",
            self.self_id,
            new_ledger.last_commit_id()
        );
        // _old drops at end of scope; its Drop joins ledger pipeline
        // threads synchronously. We're under spawn_blocking-aware
        // tokio runtime so the join is acceptable here.
    }
}

// ── Step processing ───────────────────────────────────────────────────────

/// Output of one `RaftNode::step` call, classified into reply/log/side-effect
/// buckets so the caller can act on the relevant subset.
#[derive(Default)]
struct StepOutcome {
    ae_reply: Option<Action>,
    rv_reply: Option<Action>,
    append_log: Option<LogEntryRange>,
    truncate_log: Option<u64>,
    outbound_ae: Vec<Action>,
    outbound_rv: Vec<Action>,
    wakeup: Option<Instant>,
    fatal: Option<&'static str>,
}

/// Run one `step`, snapshot the mirror, and classify the resulting
/// actions. Must be called with the raft mutex held; the mirror update
/// is sequenced before the mutex release so readers see post-step state.
fn process_step(
    node: &mut RaftNode<DurablePersistence>,
    mirror: &ClusterMirror,
    now: Instant,
    ev: Event,
) -> StepOutcome {
    let actions = node.step(now, ev);
    mirror.snapshot_from(node);
    let mut outcome = StepOutcome::default();
    for action in actions {
        match action {
            Action::SendAppendEntries { .. } => outcome.outbound_ae.push(action),
            Action::SendAppendEntriesReply { .. } => outcome.ae_reply = Some(action),
            Action::SendRequestVote { .. } => outcome.outbound_rv.push(action),
            Action::SendRequestVoteReply { .. } => outcome.rv_reply = Some(action),
            Action::TruncateLog { after_tx_id } => outcome.truncate_log = Some(after_tx_id),
            Action::AppendLog { range } => outcome.append_log = Some(range),
            // Already reflected in the mirror snapshot.
            Action::AdvanceClusterCommit { .. } => {}
            Action::BecomeRole { .. } => {}
            Action::SetWakeup { at } => outcome.wakeup = Some(at),
            Action::FatalError { reason } => {
                mirror.set_fatal();
                outcome.fatal = Some(reason);
                error!("raft_driver: FatalError emitted: {}", reason);
            }
        }
    }
    outcome
}

// ── Per-peer channel registry ─────────────────────────────────────────────

struct PeerChannels {
    peers: Vec<PeerConfig>,
    self_id: u64,
    clients: HashMap<u64, NodeClient<Channel>>,
}

impl PeerChannels {
    fn new(peers: Vec<PeerConfig>, self_id: u64) -> Self {
        Self {
            peers,
            self_id,
            clients: HashMap::new(),
        }
    }

    async fn get_or_connect(
        &mut self,
        peer_id: u64,
        max_message_bytes: usize,
    ) -> Result<&mut NodeClient<Channel>, tonic::transport::Error> {
        if !self.clients.contains_key(&peer_id) {
            let host = self
                .peers
                .iter()
                .find(|p| p.peer_id == peer_id)
                .map(|p| p.host.clone())
                .unwrap_or_else(|| {
                    panic!(
                        "raft_driver: outbound to unknown peer_id={} (not in cluster.peers)",
                        peer_id
                    )
                });
            if peer_id == self.self_id {
                panic!("raft_driver: outbound to self_id={} should not happen", peer_id);
            }
            let client = connect(&host, max_message_bytes).await?;
            self.clients.insert(peer_id, client);
        }
        Ok(self.clients.get_mut(&peer_id).unwrap())
    }

    fn invalidate(&mut self, peer_id: u64) {
        self.clients.remove(&peer_id);
    }
}

async fn connect(
    addr: &str,
    max_message_bytes: usize,
) -> Result<NodeClient<Channel>, tonic::transport::Error> {
    let endpoint = Endpoint::from_shared(addr.to_string())?;
    let channel = endpoint.connect().await?;
    Ok(NodeClient::new(channel)
        .max_decoding_message_size(max_message_bytes)
        .max_encoding_message_size(max_message_bytes))
}

// ── Wakeup task ───────────────────────────────────────────────────────────

/// Single sleeper task that fires `Event::Tick` at the latest deadline
/// emitted by `Action::SetWakeup`. Re-armed on every step that produces
/// a new wakeup; the previous task is cancelled.
struct WakeupTask {
    inner: TokioRwLock<Option<JoinHandle<()>>>,
}

impl WakeupTask {
    fn new() -> Self {
        Self {
            inner: TokioRwLock::new(None),
        }
    }

    fn reschedule(&self, at: Instant, weak_handle: std::sync::Weak<RaftHandle>) {
        // Best-effort: if the rwlock is contended (extremely rare —
        // only one writer ever and it's the dispatch path), the new
        // task spawns and the old one self-cancels on next fire because
        // the deadline match-up is loose.
        let mut slot = match self.inner.try_write() {
            Ok(s) => s,
            Err(_) => return,
        };
        if let Some(h) = slot.take() {
            h.abort();
        }
        let handle = tokio::spawn(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await;
            if let Some(h) = weak_handle.upgrade() {
                h.on_tick().await;
            }
        });
        *slot = Some(handle);
    }

    fn cancel(&self) {
        if let Ok(mut slot) = self.inner.try_write() {
            if let Some(h) = slot.take() {
                h.abort();
            }
        }
    }
}

impl Drop for WakeupTask {
    fn drop(&mut self) {
        self.cancel();
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

/// Read WAL bytes covering tx_ids `[start_tx_id, last_tx_id_inclusive]`
/// from the leader's log, bounded above by `max_bytes`. Returns the raw
/// 40-byte-per-record buffer the leader ships verbatim to the follower.
///
/// Each tx_id in the WAL spans multiple 40-byte records (Metadata +
/// Entry/Link), so the byte boundary cannot be derived from record
/// count alone. We read up to `max_bytes` from the tailer, then trim
/// to the largest prefix whose records are all `tx_id ≤
/// last_tx_id_inclusive`. SegmentHeader / SegmentSealed records carry
/// `tx_id == 0`, so the trim keeps them only when they precede or sit
/// inside the requested range.
///
/// Partial fulfillment (range > what fit in `max_bytes`) is fine —
/// raft sees the actual `last_tx_id` on the success reply and the
/// next AppendEntries continues from there.
fn read_wal_range(
    ledger: &Arc<Ledger>,
    start_tx_id: u64,
    last_tx_id_inclusive: u64,
    max_bytes: usize,
) -> Vec<u8> {
    if max_bytes < WAL_RECORD_SIZE || last_tx_id_inclusive < start_tx_id {
        return Vec::new();
    }
    // Round down to a record boundary so the tailer's per-call buffer
    // is sized to a whole number of 40-byte records.
    let aligned = max_bytes - (max_bytes % WAL_RECORD_SIZE);
    let mut tailer = ledger.wal_tailer();
    let mut buf = vec![0u8; aligned];
    let n = tailer.tail(start_tx_id, &mut buf) as usize;
    buf.truncate(n);
    trim_to_tx_id_inclusive(&buf, last_tx_id_inclusive)
}

/// Walk forward through 40-byte WAL records and return the prefix
/// whose tx_ids are all ≤ `last_tx_id_inclusive`. Records with
/// `tx_id == 0` (segment metadata) are kept. The first record with
/// `tx_id > last_tx_id_inclusive` terminates the prefix.
fn trim_to_tx_id_inclusive(bytes: &[u8], last_tx_id_inclusive: u64) -> Vec<u8> {
    let mut keep = 0usize;
    let mut off = 0usize;
    while off + WAL_RECORD_SIZE <= bytes.len() {
        let tx_id = u64::from_le_bytes(bytes[off + 8..off + 16].try_into().unwrap());
        if tx_id > last_tx_id_inclusive {
            break;
        }
        off += WAL_RECORD_SIZE;
        keep = off;
    }
    bytes[..keep].to_vec()
}

fn tx_id_of_last_record(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() || !bytes.len().is_multiple_of(WAL_RECORD_SIZE) {
        return None;
    }
    let off = bytes.len() - WAL_RECORD_SIZE + 8;
    Some(u64::from_le_bytes(bytes[off..off + 8].try_into().ok()?))
}

fn action_to_ae_response(action: Action) -> proto::AppendEntriesResponse {
    if let Action::SendAppendEntriesReply {
        term,
        success,
        last_tx_id,
        ..
    } = action
    {
        proto::AppendEntriesResponse {
            term,
            success,
            last_tx_id,
            reject_reason: if success {
                proto::RejectReason::RejectNone as u32
            } else {
                proto::RejectReason::RejectPrevMismatch as u32
            },
        }
    } else {
        proto::AppendEntriesResponse {
            term: 0,
            success: false,
            last_tx_id: 0,
            reject_reason: proto::RejectReason::RejectNotFollower as u32,
        }
    }
}

fn action_to_rv_response(action: Action) -> proto::RequestVoteResponse {
    if let Action::SendRequestVoteReply { term, granted, .. } = action {
        proto::RequestVoteResponse {
            term,
            vote_granted: granted,
        }
    } else {
        proto::RequestVoteResponse {
            term: 0,
            vote_granted: false,
        }
    }
}

fn map_proto_reject(reject: u32) -> Option<RejectReason> {
    if reject == proto::RejectReason::RejectTermStale as u32 {
        Some(RejectReason::TermBehind)
    } else if reject == proto::RejectReason::RejectPrevMismatch as u32 {
        Some(RejectReason::LogMismatch)
    } else {
        Some(RejectReason::LogMismatch)
    }
}

/// Bind the `Ledger::on_commit` slot to the driver's pending-local-commit
/// atomic. The closure is a single `fetch_max` per call — no allocation,
/// no `tokio::spawn`, no `await`. Raft picks up the value on the next
/// step (any inbound RPC, outbound reply, or wakeup tick), drains it
/// synchronously while holding the raft mutex, and feeds it as
/// `Event::LocalCommitAdvanced` if this node is currently leader.
///
/// Worst-case latency between local commit and raft seeing it is one
/// heartbeat interval (default 50 ms). Acceptable for cluster_commit_index
/// progress; far cheaper than spawning a task per commit.
pub fn register_on_commit_hook(ledger: &Arc<Ledger>, pending: Arc<AtomicU64>) {
    let handler = std::sync::Arc::new(move |tx_id: u64| {
        pending.fetch_max(tx_id, Ordering::Release);
    });
    if ledger.on_commit(handler).is_err() {
        // `Ledger::on_commit` is single-slot. Reseed re-installs against
        // the new ledger Arc; the old hook dies with the old ledger.
        // A double-register against the same ledger is a logic bug.
        panic!("raft_driver: on_commit handler already registered on this ledger");
    }
}
