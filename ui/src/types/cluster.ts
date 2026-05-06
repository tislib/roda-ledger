export type Role = 'Initializing' | 'Follower' | 'Candidate' | 'Leader';

export type NodeHealth = 'Up' | 'Stopped' | 'Partitioned' | 'Isolated';

export type ClusterHealth = 'Healthy' | 'Unhealthy';

export type FaultAction = 'stop' | 'start' | 'restart' | 'partition' | 'heal' | 'kill';

export interface NodeStatus {
  nodeId: string;
  address: string;
  role: Role;
  currentTerm: string;
  votedFor: string | null;
  health: NodeHealth;
  /** Peers this node has lost reachability to. Populated when health is Partitioned/Isolated. */
  partitionedPeers: string[];
  lastHeartbeatAt: number | null;
  computeIndex: string;
  commitIndex: string;
  snapshotIndex: string;
  clusterCommitIndex: string;
  /** For followers: how far behind leader.cluster_commit_index this node's commit_index is, in entries. 0 for leader. */
  lagEntries: string;
  /** Wall-clock ms since the last successful interaction with the leader. 0 for leader. */
  lagMs: number;
}

export type PartitionPair = readonly [string, string];

export interface ClusterSnapshot {
  takenAt: number;
  clusterHealth: ClusterHealth;
  leaderNodeId: string | null;
  currentTerm: string;
  nodes: NodeStatus[];
  partitions: PartitionPair[];
}

export type ElectionReason = 'timeout' | 'leader-crash' | 'split-vote' | 'bootstrap';

export interface ElectionEvent {
  at: number;
  term: string;
  /** null if no winner yet (split vote / ongoing). */
  winnerNodeId: string | null;
  reason: ElectionReason;
}

export type FaultKind = 'stop' | 'start' | 'restart' | 'partition' | 'heal' | 'kill';

export interface FaultEvent {
  at: number;
  kind: FaultKind;
  nodeId: string;
  /** Only set for partition/heal. */
  peerNodeId: string | null;
  description: string;
}

export interface NodeInfo {
  nodeId: string;
  address: string;
}

export interface ClusterMembership {
  nodes: NodeInfo[];
  targetCount: number;
}

export interface ClusterConfig {
  maxAccounts: string;
  queueSize: string;
  transactionCountPerSegment: string;
  snapshotFrequency: number;
  replicationPollMs: string;
  appendEntriesMaxBytes: string;
}

export interface ServerInfo {
  version: string;
  apiVersion: number;
  capabilities: string[];
}
