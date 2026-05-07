import { useQuery } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { qk } from '@/lib/query-keys';

export function useNodeLog(
  nodeId: string | null,
  fromIndex: string,
  limit: number,
  intervalMs = 1000,
) {
  const client = useClusterClient();
  return useQuery({
    queryKey: nodeId ? qk.cluster.log(nodeId, fromIndex) : ['cluster', 'log', null],
    queryFn: () =>
      nodeId
        ? client.getNodeLog(nodeId, { fromIndex, limit })
        : Promise.resolve({
            entries: [],
            totalCount: '0',
            nextFromIndex: '0',
            oldestRetainedIndex: '0',
          }),
    refetchInterval: nodeId ? intervalMs : false,
    enabled: nodeId != null,
    placeholderData: (prev) => prev,
  });
}
