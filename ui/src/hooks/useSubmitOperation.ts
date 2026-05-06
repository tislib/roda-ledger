import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { qk } from '@/lib/query-keys';
import type { Operation } from '@/types/transaction';

export function useSubmitOperation() {
  const client = useClusterClient();
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (op: Operation) => client.submitOperation(op),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: qk.cluster.snapshot() });
    },
  });
}
