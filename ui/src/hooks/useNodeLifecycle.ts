import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { qk } from '@/lib/query-keys';
import type { LifecycleAction } from '@/types/cluster';

export function useNodeLifecycle() {
  const client = useClusterClient();
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ nodeId, action }: { nodeId: string; action: LifecycleAction }) =>
      client.applyLifecycle(nodeId, action),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: qk.cluster.snapshot() });
    },
  });
}

export function usePartition() {
  const client = useClusterClient();
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ a, b, heal }: { a: string; b: string; heal: boolean }) =>
      heal ? client.healPartition(a, b) : client.partition(a, b),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: qk.cluster.snapshot() });
    },
  });
}
