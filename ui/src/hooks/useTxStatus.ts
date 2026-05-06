import { useQuery } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { qk } from '@/lib/query-keys';
import { isTerminal } from '@/types/transaction';

export function useTxStatus(txId: string | null) {
  const client = useClusterClient();
  return useQuery({
    queryKey: txId ? qk.tx.status(txId) : ['tx', 'status', null],
    queryFn: () =>
      txId ? client.getTransactionStatus(txId) : Promise.resolve({ state: 'NotFound' as const }),
    refetchInterval: (query) => {
      const data = query.state.data;
      if (!data || isTerminal(data)) return false;
      return 250;
    },
    enabled: txId != null,
  });
}
