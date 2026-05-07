import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { qk } from '@/lib/query-keys';

export function useRegisterFunction() {
  const client = useClusterClient();
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      name,
      source,
      overrideExisting,
    }: {
      name: string;
      source: string;
      overrideExisting: boolean;
    }) => client.registerFunction(name, source, overrideExisting),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: qk.cluster.snapshot() });
      qc.invalidateQueries({ queryKey: qk.wasm.list() });
    },
  });
}

export function useUnregisterFunction() {
  const client = useClusterClient();
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => client.unregisterFunction(name),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: qk.wasm.list() });
    },
  });
}
