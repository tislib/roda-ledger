import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { qk } from '@/lib/query-keys';

export function useFaultHistory(limit = 64, intervalMs = 1000) {
  const client = useClusterClient();
  return useQuery({
    queryKey: qk.cluster.faults(),
    queryFn: () => client.getFaultHistory(limit),
    refetchInterval: intervalMs,
    placeholderData: (prev) => prev,
  });
}

export function useStopNode() {
  const client = useClusterClient();
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (nodeId: string) => client.stopNode(nodeId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: qk.cluster.snapshot() });
      qc.invalidateQueries({ queryKey: qk.cluster.faults() });
    },
  });
}

export function useStartNode() {
  const client = useClusterClient();
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (nodeId: string) => client.startNode(nodeId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: qk.cluster.snapshot() });
      qc.invalidateQueries({ queryKey: qk.cluster.faults() });
    },
  });
}

export function useRestartNode() {
  const client = useClusterClient();
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (nodeId: string) => client.restartNode(nodeId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: qk.cluster.snapshot() });
      qc.invalidateQueries({ queryKey: qk.cluster.faults() });
    },
  });
}

export function usePartitionPair() {
  const client = useClusterClient();
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ a, b, heal }: { a: string; b: string; heal: boolean }) =>
      heal ? client.healPartition(a, b) : client.partitionPair(a, b),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: qk.cluster.snapshot() });
      qc.invalidateQueries({ queryKey: qk.cluster.faults() });
    },
  });
}
