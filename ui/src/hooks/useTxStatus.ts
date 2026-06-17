import { useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { qk } from '@/lib/query-keys';
import { usePipelineIndices } from '@/hooks/usePipelineIndices';
import { parseTxId, stageFromIndices } from '@/lib/tx-stage';
import type { FailReasonCode, TransactionStatus } from '@/types/transaction';

// Module-scoped so failReason stays sticky across re-renders, remounts,
// and across components that subscribe to the same txId. Once a non-zero
// reason is observed it is never overwritten.
const stickyFailReason = new Map<string, FailReasonCode>();

interface UseTxStatusResult {
  data: TransactionStatus | null;
}

export function useTxStatus(txId: string | null): UseTxStatusResult {
  const client = useClusterClient();
  const indices = usePipelineIndices();

  const stage = txId ? stageFromIndices(parseTxId(txId), indices) : 'Pending';
  const captured = txId ? (stickyFailReason.get(txId) ?? 0) : 0;
  const reachedSnapshot = stage === 'OnSnapshot';
  // Pre-snapshot polling covers the mock (failed txs settle in an Error
  // stage); the real backend only exposes the reason once the tx is on the
  // snapshot, so re-key on `reachedSnapshot` to force one fetch there.
  const shouldPoll = txId != null && captured === 0 && !reachedSnapshot;
  const enabled = txId != null && captured === 0;

  const q = useQuery({
    queryKey: txId ? [...qk.tx.status(txId), reachedSnapshot] : ['tx', 'status', null],
    queryFn: async (): Promise<{ failReason: FailReasonCode }> => {
      if (!txId) return { failReason: 0 };
      return client.getTransactionStatus(txId);
    },
    refetchInterval: shouldPoll ? 500 : false,
    enabled,
  });

  useEffect(() => {
    if (txId && q.data && q.data.failReason !== 0) {
      stickyFailReason.set(txId, q.data.failReason);
    }
  }, [txId, q.data]);

  if (!txId) return { data: null };

  const failReason = captured !== 0 ? captured : (q.data?.failReason ?? 0);
  return { data: { txId, stage, failReason } };
}
