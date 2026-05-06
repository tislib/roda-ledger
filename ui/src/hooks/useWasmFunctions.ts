import { useQuery } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { qk } from '@/lib/query-keys';

export function useWasmFunctions(intervalMs = 2000) {
  const client = useClusterClient();
  return useQuery({
    queryKey: qk.wasm.list(),
    queryFn: () => client.listFunctions(),
    refetchInterval: intervalMs,
    placeholderData: (prev) => prev,
  });
}
