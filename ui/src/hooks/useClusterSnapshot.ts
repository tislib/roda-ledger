import { useQuery } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { qk } from '@/lib/query-keys';

export function useClusterSnapshot(intervalMs = 500) {
  const client = useClusterClient();
  // If the active client supports streaming, the App-level
  // `useClusterSnapshotStream` is feeding the cache live — disable the
  // polling loop. Otherwise (e.g. mock://local), poll as before.
  const streaming = typeof client.watchClusterSnapshot === 'function';
  return useQuery({
    queryKey: qk.cluster.snapshot(),
    queryFn: () => client.getClusterSnapshot(),
    refetchInterval: streaming ? false : intervalMs,
    placeholderData: (prev) => prev,
    // Streams may not have emitted yet; do an initial fetch so the page
    // doesn't stay blank while waiting for the first frame.
    enabled: true,
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
