import { useQuery } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { qk } from '@/lib/query-keys';

export function useWasmFunctions(intervalMs = 2000) {
  const client = useClusterClient();
  const streaming = typeof client.watchFunctions === 'function';
  return useQuery({
    queryKey: qk.wasm.list(),
    queryFn: () => client.listFunctions(),
    refetchInterval: streaming ? false : intervalMs,
    placeholderData: (prev) => prev,
  });
}
