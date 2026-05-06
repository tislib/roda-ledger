import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { qk } from '@/lib/query-keys';
import type { ClusterConfig } from '@/types/cluster';

export function useClusterConfig(intervalMs = 5000) {
  const client = useClusterClient();
  return useQuery({
    queryKey: qk.cluster.config(),
    queryFn: () => client.getClusterConfig(),
    refetchInterval: intervalMs,
    staleTime: 1000,
  });
}

export function useUpdateClusterConfig() {
  const client = useClusterClient();
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (config: ClusterConfig) => client.updateClusterConfig(config),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: qk.cluster.config() });
      qc.invalidateQueries({ queryKey: qk.cluster.snapshot() });
    },
  });
}

export function useSetNodeCount() {
  const client = useClusterClient();
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (target: number) => client.setNodeCount(target),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: qk.cluster.config() });
      qc.invalidateQueries({ queryKey: qk.cluster.snapshot() });
    },
  });
}
