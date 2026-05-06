import { useQuery } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { qk } from '@/lib/query-keys';

export function useClusterSnapshot(intervalMs = 500) {
  const client = useClusterClient();
  return useQuery({
    queryKey: qk.cluster.snapshot(),
    queryFn: () => client.getClusterSnapshot(),
    refetchInterval: intervalMs,
    placeholderData: (prev) => prev,
  });
}

export function useRecentElections(limit = 12, intervalMs = 1000) {
  const client = useClusterClient();
  return useQuery({
    queryKey: qk.cluster.elections(),
    queryFn: () => client.getRecentElections(limit),
    refetchInterval: intervalMs,
    placeholderData: (prev) => prev,
  });
}

export function useServerInfo() {
  const client = useClusterClient();
  return useQuery({
    queryKey: qk.server.info(),
    queryFn: () => client.getServerInfo(),
    staleTime: 60_000,
  });
}
