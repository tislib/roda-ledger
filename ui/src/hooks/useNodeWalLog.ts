import { useQuery } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';

export function useNodeWalLog(
  nodeId: string | null,
  fromTxId: string,
  limit: number,
  intervalMs = 1000,
) {
  const client = useClusterClient();
  return useQuery({
    queryKey: nodeId
      ? ['cluster', 'walLog', nodeId, fromTxId, limit]
      : ['cluster', 'walLog', null],
    queryFn: () =>
      nodeId
        ? client.getNodeWalLog(nodeId, { fromTxId, limit })
        : Promise.resolve({ records: [], nextTxId: '0', lastCommitTxId: '0' }),
    refetchInterval: nodeId ? intervalMs : false,
    enabled: nodeId != null,
    placeholderData: (prev) => prev,
  });
}
