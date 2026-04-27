//! `RoleSupervisor` — owns the long-lived gRPC servers + the shared
//! role/state atomics, drives the role state machine, and reseeds
//! the embedded `Ledger` on divergence detection (ADR-0016 §2,
//! §3, §5, §9).
//!
//! Stage 4 layout. The supervisor's `run()`:
//!
//! 1. Spawns the long-lived **client** + **node** gRPC servers
//!    (these stay up across every role transition).
//! 2. Spawns a **divergence watcher** that polls
//!    `NodeHandlerCore::take_divergence_watermark` and triggers
//!    [`reseed`] on `Some`.
//! 3. Spawns the supervised **role driver** task — a long-running
//!    `loop { match role { ... } }` that:
//!    - In `Initializing` / `Follower`, awaits the election timer.
//!      Expiry → transition to `Candidate`.
//!    - In `Candidate`, runs one election round
//!      ([`crate::cluster::raft::run_election_round`]).
//!      `Won` → `Leader`. `HigherTermSeen` → `Initializing` (and
//!      `Term::observe` is performed inside the round).
//!      `Lost` → loop back to `Initializing` (re-arms timer).
//!    - In `Leader`, spawns role-specific tasks
//!      (peer replication, on-commit hook). The driver awaits
//!      either a step-down signal (a peer responded with a higher
//!      term, or AppendEntries observed a higher term) or process
//!      shutdown. On step-down, peer tasks are drained and the
//!      driver re-enters `Initializing`.
//!
//! Transitions other than the timer-driven one are signalled
//! through a [`tokio::sync::mpsc`] channel
//! ([`TransitionTx`] / [`TransitionRx`]) — every gRPC handler that
//! observes a higher term, every divergence-detected reseed,
//! pushes a [`Transition`] enum value.

use crate::cluster::config::Config;
use crate::cluster::node_handler::NodeHandlerCore;
use crate::cluster::raft::{
    ElectionOutcome, ElectionTimer, ElectionTimerConfig, Leader, Quorum, Role, RoleFlag, Term,
    Vote, run_election_round,
};
use crate::cluster::server::{NodeServerRuntime, Server};
use crate::cluster::{LedgerSlot, NodeHandler};
use crate::ledger::Ledger;
use spdlog::{debug, error, info, warn};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Events that drive the role state machine. Beyond timer expiry
/// (handled inline by the role driver), every other transition is a
/// `Transition` value sent on the supervisor's mpsc channel.
#[derive(Debug, Clone, Copy)]
pub enum Transition {
    /// Some RPC observed a peer term strictly above ours; we must
    /// step down to Initializing immediately. The supervisor's role
    /// driver re-reads `term.get_current_term()` after stepping
    /// down (term observation is durable before the signal fires).
    StepDownHigherTerm { observed: u64 },
    /// `NodeHandlerCore::take_divergence_watermark` returned `Some`
    /// — supervisor's reseed loop will catch this via its own
    /// poller, but in the future Stage 4 may also signal here so
    /// the role driver can drain peer tasks before reseed.
    #[allow(dead_code)]
    DivergenceDetected { watermark: u64 },
    /// Process shutdown — drain everything and exit the role loop.
    Shutdown,
}

pub type TransitionTx = mpsc::Sender<Transition>;
pub type TransitionRx = mpsc::Receiver<Transition>;

/// Output of [`RoleSupervisor::run`].
pub struct SupervisorHandles {
    pub client_handle: JoinHandle<()>,
    pub node_handle: JoinHandle<()>,
    pub watcher_handle: JoinHandle<()>,
    /// The role-driver task — a long-running loop that owns role
    /// transitions and per-role bring-up.
    pub driver_handle: JoinHandle<()>,
    /// Cooperative shutdown flag for peer-replication sub-tasks
    /// owned transiently by the driver in the Leader role.
    pub running: Arc<AtomicBool>,
    /// Sender side of the transition channel — exposed for handlers
    /// that need to signal step-down.
    pub transition_tx: TransitionTx,
    /// Long-lived cluster-wide majority tracker. Survives Leader
    /// transitions; exposed for observability (`load_cluster`,
    /// tests) and the future quorum-gated commit path.
    pub quorum: Arc<Quorum>,
    /// Shared NodeHandler state. `abort` flips its `shutdown` latch so
    /// any in-flight or subsequent gRPC handler refuses immediately —
    /// this is the in-process stand-in for "the process is gone" and
    /// is what makes the dying-follower's slot in the leader's
    /// `Quorum` actually stop refreshing on soft-abort. (Hard crashes
    /// don't need this — they kill all handler tasks instantly.)
    pub node_core: Arc<crate::cluster::NodeHandlerCore>,
}

impl SupervisorHandles {
    pub fn abort(&self) {
        // Flip the gRPC-handler shutdown latch FIRST. After this
        // store, every handler invocation on this node returns
        // `Status::unavailable` — including any that the leader's
        // `PeerReplication` is about to fire and any that arrive on
        // already-accepted connections after the listener task is
        // aborted. This is what severs the dying-follower's ability
        // to keep its slot fresh in the leader's Quorum.
        self.node_core
            .shutdown
            .store(true, std::sync::atomic::Ordering::Release);
        self.running
            .store(false, std::sync::atomic::Ordering::Release);
        // A polite shutdown nudge for the driver loop in case it's
        // mid-await on the transition channel.
        let _ = self.transition_tx.try_send(Transition::Shutdown);
        self.driver_handle.abort();
        self.watcher_handle.abort();
        self.client_handle.abort();
        self.node_handle.abort();
    }
}

pub struct RoleSupervisor {
    config: Config,
    ledger_slot: Arc<LedgerSlot>,
    term: Arc<Term>,
    vote: Arc<Vote>,
    role: Arc<RoleFlag>,
}

impl RoleSupervisor {
    pub fn new(
        config: Config,
        ledger_slot: Arc<LedgerSlot>,
        term: Arc<Term>,
    ) -> std::io::Result<Self> {
        let vote = Arc::new(Vote::open_in_dir(&config.ledger.storage.data_dir)?);
        let initial_role = match config.cluster.as_ref() {
            Some(cluster) if cluster.peers.len() == 1 => Role::Leader,
            Some(_) => Role::Initializing,
            None => Role::Initializing,
        };
        Ok(Self {
            config,
            ledger_slot,
            term,
            vote,
            role: Arc::new(RoleFlag::new(initial_role)),
        })
    }

    pub async fn run(&self) -> Result<SupervisorHandles, Box<dyn std::error::Error + Send + Sync>> {
        let cluster = self
            .config
            .cluster
            .as_ref()
            .expect("RoleSupervisor::run requires a clustered config");

        let client_addr = self.config.server.socket_addr()?;
        let node_addr = cluster.node.socket_addr()?;

        // Long-lived cluster-wide quorum tracker — sized once per
        // process. Per-node-id slot positions match
        // `config.cluster.peers` ordering.
        let quorum = Arc::new(Quorum::new(self.config.cluster_size()));
        let cluster_commit_index = quorum.cluster_commit_index();

        // Self's slot in the quorum. Used by the on-commit hook
        // (this node's commit progress) and by Leader::reset_peers
        // (everything *except* this slot is wiped on re-election).
        let self_id = self.config.node_id();
        let self_slot: u32 = cluster
            .peers
            .iter()
            .position(|p| p.peer_id == self_id)
            .expect("validate() guarantees self is present in cluster.peers")
            as u32;

        // Register the on-commit hook on the initial ledger.
        // Reseed will re-register on every new ledger via
        // `register_on_commit_hook` below.
        register_on_commit_hook(&self.ledger_slot.ledger(), &quorum, self_slot);

        // Single shared election timer. The role driver's
        // Initializing/Follower arms `await_expiry` on this
        // instance; the NodeHandler resets it on every valid
        // `AppendEntries` and granted `RequestVote`. Built here so
        // both paths capture the same `Arc<ElectionTimer>`.
        let election_timer = Arc::new(ElectionTimer::new(ElectionTimerConfig::default()));

        let node_core = Arc::new(
            NodeHandlerCore::new(
                self.ledger_slot.clone(),
                self.config.node_id(),
                self.term.clone(),
                self.vote.clone(),
                self.role.clone(),
                Some(cluster_commit_index.clone()),
            )
            .with_election_timer(election_timer.clone()),
        );

        let client_server = Server::new(
            self.ledger_slot.clone(),
            client_addr,
            self.role.clone(),
            self.term.clone(),
            cluster_commit_index.clone(),
        );
        let client_handle = tokio::spawn(async move {
            if let Err(e) = client_server.run().await {
                error!("supervisor: ledger gRPC server exited: {}", e);
            }
        });

        let node_handler = NodeHandler::new(node_core.clone());
        let node_max_bytes = cluster.append_entries_max_bytes * 2 + 4 * 1024;
        let node_runtime = NodeServerRuntime::new(node_addr, node_handler, node_max_bytes);
        let node_handle = tokio::spawn(async move {
            if let Err(e) = node_runtime.run().await {
                error!("supervisor: node gRPC server exited: {}", e);
            }
        });

        let running = Arc::new(AtomicBool::new(true));

        // Transition channel sized for "small bursts" — at most a
        // handful of RPCs in flight simultaneously can each push.
        let (transition_tx, transition_rx) = mpsc::channel::<Transition>(16);

        // Divergence watcher (ADR-0016 §9) — independent of the
        // role driver so a Leader getting reseeded can react
        // promptly without the driver having to be in the right
        // state to listen.
        let watcher_handle = self.spawn_divergence_watcher(
            node_core.clone(),
            running.clone(),
            quorum.clone(),
            self_slot,
        );

        // Role driver — the main state machine. The driver also
        // gets a clone of `transition_tx` so the Leader bring-up can
        // hand it to each `PeerReplication` task: a peer that
        // observes a higher term in an `AppendEntries` response
        // pushes `Transition::StepDownHigherTerm` back here.
        let driver_handle = self.spawn_role_driver(
            transition_rx,
            running.clone(),
            quorum.clone(),
            transition_tx.clone(),
            election_timer.clone(),
        );

        Ok(SupervisorHandles {
            client_handle,
            node_handle,
            watcher_handle,
            driver_handle,
            running,
            transition_tx,
            quorum,
            node_core,
        })
    }

    fn spawn_divergence_watcher(
        &self,
        node_core: Arc<NodeHandlerCore>,
        running: Arc<AtomicBool>,
        quorum: Arc<Quorum>,
        self_slot: u32,
    ) -> JoinHandle<()> {
        let config = self.config.clone();
        let ledger_slot = self.ledger_slot.clone();
        let role = self.role.clone();
        let self_id = self.config.node_id();
        tokio::spawn(async move {
            debug!(
                "supervisor[node_id={}]: divergence watcher started (poll interval {:?})",
                self_id,
                Self::WATCHER_INTERVAL
            );
            while running.load(std::sync::atomic::Ordering::Relaxed) {
                if let Some(watermark) = node_core.take_divergence_watermark() {
                    info!(
                        "supervisor[node_id={}]: divergence detected (watermark={}); reseeding ledger",
                        self_id, watermark
                    );
                    if let Err(e) =
                        reseed(&config, &ledger_slot, &role, &quorum, self_slot, watermark).await
                    {
                        error!("supervisor[node_id={}]: reseed failed: {}", self_id, e);
                    } else {
                        info!(
                            "supervisor[node_id={}]: reseed complete (last_commit_id now={})",
                            self_id,
                            ledger_slot.ledger().last_commit_id()
                        );
                    }
                }
                tokio::time::sleep(Self::WATCHER_INTERVAL).await;
            }
            debug!(
                "supervisor[node_id={}]: divergence watcher exiting (running flag cleared)",
                self_id
            );
        })
    }

    fn spawn_role_driver(
        &self,
        mut transition_rx: TransitionRx,
        running: Arc<AtomicBool>,
        quorum: Arc<Quorum>,
        transition_tx_for_driver: TransitionTx,
        timer: Arc<ElectionTimer>,
    ) -> JoinHandle<()> {
        let config = self.config.clone();
        let ledger_slot = self.ledger_slot.clone();
        let term = self.term.clone();
        let vote = self.vote.clone();
        let role = self.role.clone();

        tokio::spawn(async move {
            let self_id = config.node_id();
            let mut iter = 0u64;
            loop {
                if !running.load(std::sync::atomic::Ordering::Relaxed) {
                    debug!(
                        "supervisor[node_id={}]: role driver exiting after {} iterations (running flag cleared)",
                        self_id, iter
                    );
                    return;
                }
                iter += 1;
                let current = role.get();
                debug!(
                    "supervisor[node_id={}]: role driver iter={} state={:?} term={}",
                    self_id,
                    iter,
                    current,
                    term.get_current_term()
                );
                match current {
                    Role::Leader => {
                        run_leader_role(
                            &config,
                            &ledger_slot,
                            &term,
                            &role,
                            &quorum,
                            &mut transition_rx,
                            &running,
                            transition_tx_for_driver.clone(),
                        )
                        .await;
                    }
                    Role::Initializing | Role::Follower => {
                        // Wait for either the election timer to
                        // expire OR a transition signal to arrive.
                        debug!(
                            "supervisor[node_id={}]: arming election timer in {:?} state",
                            self_id, current
                        );
                        timer.reset();
                        tokio::select! {
                            _ = timer.await_expiry() => {
                                debug!(
                                    "supervisor[node_id={}]: election timer expired in {:?} — promoting to Candidate",
                                    self_id, current
                                );
                                role.set(Role::Candidate);
                            }
                            ev = transition_rx.recv() => {
                                debug!(
                                    "supervisor[node_id={}]: transition signal received in {:?}: {:?}",
                                    self_id, current, ev
                                );
                                handle_transition(ev, &role, &term);
                            }
                        }
                    }
                    Role::Candidate => {
                        // Cap the round duration around the upper
                        // half of the configured election timeout
                        // window — long enough for one normal
                        // round-trip on healthy networks, short
                        // enough that a partitioned candidate spins
                        // back to Initializing quickly.
                        let round_deadline =
                            Duration::from_millis(ElectionTimerConfig::default().max_ms);
                        let live_ledger = ledger_slot.ledger();
                        match run_election_round(
                            &config,
                            &live_ledger,
                            &term,
                            &vote,
                            round_deadline,
                        )
                        .await
                        {
                            Ok(ElectionOutcome::Won { elected_term }) => {
                                info!(
                                    "supervisor: node_id={} won election in term {}",
                                    config.node_id(),
                                    elected_term
                                );
                                role.set(Role::Leader);
                            }
                            Ok(ElectionOutcome::HigherTermSeen { observed_term }) => {
                                if let Err(e) = term
                                    .observe(observed_term, ledger_slot.ledger().last_commit_id())
                                {
                                    warn!(
                                        "supervisor: term.observe({}) on step-down failed: {}",
                                        observed_term, e
                                    );
                                }
                                if let Err(e) = vote.observe_term(observed_term) {
                                    warn!(
                                        "supervisor: vote.observe_term({}) on step-down failed: {}",
                                        observed_term, e
                                    );
                                }
                                role.set(Role::Initializing);
                            }
                            Ok(ElectionOutcome::Lost) => {
                                // Re-arm the timer and try again.
                                role.set(Role::Initializing);
                            }
                            Err(e) => {
                                warn!("supervisor: election round errored: {}", e);
                                role.set(Role::Initializing);
                            }
                        }
                    }
                }
            }
        })
    }

    const WATCHER_INTERVAL: Duration = Duration::from_millis(10);
}

fn handle_transition(ev: Option<Transition>, role: &Arc<RoleFlag>, _term: &Arc<Term>) {
    match ev {
        Some(Transition::StepDownHigherTerm { observed }) => {
            warn!("supervisor: step down observed term={}", observed);
            role.set(Role::Initializing);
        }
        Some(Transition::DivergenceDetected { watermark }) => {
            info!(
                "supervisor: transition channel reports divergence watermark={}",
                watermark
            );
            role.set(Role::Initializing);
        }
        Some(Transition::Shutdown) | None => {
            // Closed channel or explicit shutdown — caller-driven
            // exit. The outer loop's `running` flag check will
            // handle cleanup.
        }
    }
}

async fn run_leader_role(
    config: &Config,
    ledger_slot: &Arc<LedgerSlot>,
    term: &Arc<Term>,
    role: &Arc<RoleFlag>,
    quorum: &Arc<Quorum>,
    transition_rx: &mut TransitionRx,
    running: &Arc<AtomicBool>,
    transition_tx: TransitionTx,
) {
    // Leader-scoped cancellation flag. Per-peer replication tasks
    // poll this and exit when it flips false. Distinct from the
    // process-wide `running` (which signals supervisor shutdown):
    // step-down must terminate peer tasks WITHOUT taking the rest
    // of the supervisor down with them.
    let leader_alive = Arc::new(AtomicBool::new(true));

    // Observe term snapshot at bring-up. After step-down we
    // re-observe it for the next Leader entry.
    let observed_term_at_entry = term.get_current_term();

    let leader = Leader::new(
        config.clone(),
        ledger_slot.clone(),
        term.clone(),
        quorum.clone(),
    );
    let leader_handles = match leader
        .run_role_tasks(leader_alive.clone(), running.clone(), transition_tx.clone())
        .await
    {
        Ok(h) => h,
        Err(e) => {
            warn!("supervisor: leader bring-up failed: {}", e);
            role.set(Role::Initializing);
            return;
        }
    };
    info!(
        "supervisor: node_id={} entered Leader (peers={}, term={})",
        config.node_id(),
        config.cluster_size(),
        observed_term_at_entry
    );

    // Stay in Leader until either a transition signals step-down
    // or the process is shutting down.
    let mut step_down_observed: Option<u64> = None;
    while running.load(std::sync::atomic::Ordering::Relaxed) {
        debug!(
            "supervisor[node_id={}]: leader awaiting transition signal (term={})",
            config.node_id(),
            term.get_current_term()
        );
        match transition_rx.recv().await {
            Some(Transition::StepDownHigherTerm { observed }) => {
                warn!(
                    "supervisor[node_id={}]: leader stepping down (observed term {})",
                    config.node_id(),
                    observed
                );
                step_down_observed = Some(observed);
                break;
            }
            Some(Transition::DivergenceDetected { .. }) => {
                // Followers are the only nodes that detect
                // divergence under ADR-0016 §9; ignore on the
                // leader. (If we ever see it, fall through to
                // step-down to be safe.)
                warn!(
                    "supervisor[node_id={}]: leader observed unexpected divergence; stepping down",
                    config.node_id()
                );
                break;
            }
            Some(Transition::Shutdown) | None => {
                debug!(
                    "supervisor[node_id={}]: leader received shutdown signal — exiting role",
                    config.node_id()
                );
                break;
            }
        }
    }

    // Cooperative drain: tell every peer task to exit, then await.
    debug!(
        "supervisor[node_id={}]: draining {} peer-replication tasks",
        config.node_id(),
        leader_handles.peer_handles.len()
    );
    leader_alive.store(false, std::sync::atomic::Ordering::Release);
    for h in leader_handles.peer_handles {
        let _ = h.await;
    }
    debug!(
        "supervisor[node_id={}]: peer-replication tasks drained",
        config.node_id()
    );

    // After draining peers, persist the higher term we just
    // observed (if any). The matching `vote.observe_term` lives on
    // the role driver's `Candidate`-state arm, where it's already
    // synchronised with the role atomic.
    if let Some(observed) = step_down_observed {
        if let Err(e) = term.observe(observed, ledger_slot.ledger().last_commit_id()) {
            warn!(
                "supervisor: term.observe({}) on leader step-down failed: {}",
                observed, e
            );
        }
    }
    role.set(Role::Initializing);
}

/// Build a fresh `Ledger`, run `start_with_recovery_until(watermark)`,
/// re-register the `on_commit` hook so the supervisor's quorum
/// keeps tracking commit progress across the swap, and atomically
/// install the new `Arc<Ledger>` into the slot.
async fn reseed(
    config: &Config,
    ledger_slot: &Arc<LedgerSlot>,
    role: &Arc<RoleFlag>,
    quorum: &Arc<Quorum>,
    self_slot: u32,
    watermark: u64,
) -> Result<(), std::io::Error> {
    let prior_role = role.get();
    role.set(Role::Initializing);

    let ledger_cfg = config.ledger.clone();
    let new_ledger: Arc<Ledger> =
        tokio::task::spawn_blocking(move || -> Result<Arc<Ledger>, std::io::Error> {
            let mut ledger = Ledger::new(ledger_cfg);
            ledger.start_with_recovery_until(watermark)?;
            Ok(Arc::new(ledger))
        })
        .await
        .map_err(|e| std::io::Error::other(format!("reseed: spawn_blocking panicked: {}", e)))??;

    // Re-register the on-commit hook on the *new* ledger so the
    // supervisor's permanent quorum keeps observing commit
    // advances after the swap. The old ledger's hook dies with it
    // when its `Arc` refcount hits zero.
    register_on_commit_hook(&new_ledger, quorum, self_slot);

    let _old = ledger_slot.replace(new_ledger);

    if matches!(prior_role, Role::Leader) {
        role.set(Role::Leader);
    } else {
        warn!(
            "supervisor: reseed left node in Initializing (prior role {:?})",
            prior_role
        );
    }
    Ok(())
}

/// Install the supervisor's commit hook on `ledger`. Called once
/// per ledger lifetime — at supervisor startup against the initial
/// ledger, then again inside `reseed` for each replacement. The
/// closure captures only `Arc<Quorum>` + `self_slot`, both of which
/// outlive any individual ledger.
fn register_on_commit_hook(ledger: &Arc<Ledger>, quorum: &Arc<Quorum>, self_slot: u32) {
    let q = quorum.clone();
    if ledger
        .on_commit(Arc::new(move |tx_id| q.advance(self_slot, tx_id)))
        .is_err()
    {
        // `Ledger::on_commit` is single-slot per ledger; this would
        // only fire if someone else registered first. Loud panic so
        // we don't silently lose commit observations.
        panic!("supervisor: on_commit handler already registered on this ledger");
    }
    quorum.advance(self_slot, ledger.last_commit_id());
}
