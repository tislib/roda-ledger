import { useRef } from 'react';
import { useClusterSnapshot } from '@/hooks/useClusterSnapshot';
import type { PipelineIndices } from '@/lib/tx-stage';

/**
 * Returns the leader's pipeline indices (compute/commit/snapshot/cluster_commit).
 *
 * Sticky: if the cluster is briefly leader-less (mid-election), the last
 * known indices are kept so the pipeline display freezes rather than
 * collapsing to Pending mid-flight.
 *
 * The underlying `useClusterSnapshot` query is shared (TanStack Query
 * dedup), so calling this from multiple subscribers does not multiply load.
 */
export function usePipelineIndices(): PipelineIndices | null {
  const { data: snap } = useClusterSnapshot();
  const lastRef = useRef<PipelineIndices | null>(null);

  if (snap?.leaderNodeId) {
    const leader = snap.nodes.find((n) => n.nodeId === snap.leaderNodeId);
    if (leader) {
      lastRef.current = {
        compute: BigInt(leader.computeIndex),
        commit: BigInt(leader.commitIndex),
        snapshot: BigInt(leader.snapshotIndex),
        clusterCommit: BigInt(leader.clusterCommitIndex),
      };
    }
  }
  return lastRef.current;
}
