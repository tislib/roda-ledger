import type { LogEntry } from '@/types/log';
import type { Role } from '@/types/cluster';

/** Internal simulator-only health: just the process-level distinction. Network status (partitioned/isolated) is derived from PartitionMatrix at snapshot time. */
export type InternalHealth = 'Up' | 'Stopped';

export interface SimLogEntry extends LogEntry {
  /** Tx id this entry was created from, if any (Function/ledger ops). */
  txId?: string;
  /** Whether this entry has been committed locally (used by simulator only). */
  committed: boolean;
}

export class RaftNodeState {
  readonly nodeId: string;

  // Volatile + persistent (mock collapses both into one place).
  role: Role = 'Follower';
  currentTerm = 0n;
  votedFor: string | null = null;
  currentLeader: string | null = null;

  // Log replication state.
  log: SimLogEntry[] = []; // ordered by index ascending; index = entry.index
  writeIndex = 0n; // local durable extent (count of appended entries)
  // Three independent staged-pipeline watermarks. Advance at staggered timing
  // so the Dashboard's IndexBars genuinely move at different rates — not all
  // aliased to a single value.
  computeIndex = 0n;
  commitIndex = 0n;
  snapshotIndex = 0n;
  clusterCommitIndex = 0n; // quorum-committed (leader-driven)

  // Election timing.
  electionDeadline = 0; // absolute ms; 0 = none scheduled
  lastHeartbeatAt: number | null = null;

  // Vote bookkeeping for current term (leader/candidate).
  votesReceived = new Set<string>();

  // Per-follower next index, only relevant on the leader.
  nextIndex = new Map<string, bigint>();
  matchIndex = new Map<string, bigint>();

  // Process-level health (mock layer). Network state derived elsewhere.
  health: InternalHealth = 'Up';

  constructor(nodeId: string) {
    this.nodeId = nodeId;
  }

  termAsString(): string {
    return this.currentTerm.toString();
  }

  writeIndexAsString(): string {
    return this.writeIndex.toString();
  }

  commitIndexAsString(): string {
    return this.commitIndex.toString();
  }

  computeIndexAsString(): string {
    return this.computeIndex.toString();
  }

  snapshotIndexAsString(): string {
    return this.snapshotIndex.toString();
  }

  clusterCommitIndexAsString(): string {
    return this.clusterCommitIndex.toString();
  }
}
