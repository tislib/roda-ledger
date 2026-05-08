import { useMutation } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';

interface BalanceResult {
  balance: string;
  lastSnapshotTxId: string;
}

export function useGetBalance() {
  const client = useClusterClient();
  return useMutation<BalanceResult, Error, { accountId: string; nodeId: string }>({
    mutationFn: ({ accountId, nodeId }) => client.getBalance(accountId, nodeId),
  });
}
