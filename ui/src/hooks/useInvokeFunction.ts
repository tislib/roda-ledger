import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { qk } from '@/lib/query-keys';
import type { WasmFunction } from '@/types/wasm';

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
    // Optimistically push the new entry into the cached function list so
    // the UI shows it as "deployed" immediately, without waiting for the
    // next listFunctions response. (The cluster takes a moment to commit
    // the registration; without this the entry would briefly bounce out
    // of the deployed list when polling refetches.)
    onSuccess: (_resp, vars) => {
      qc.setQueryData<WasmFunction[]>(qk.wasm.list(), (prev) => {
        const next = (prev ?? []).filter((f) => f.name !== vars.name);
        next.push({
          name: vars.name,
          deployedAt: Date.now(),
          sourceLanguage: 'rust',
          source: '',
          description: 'pending sync',
          paramHints: [],
          defaultParams: ['0', '0', '0', '0', '0', '0', '0', '0'] as const,
        });
        return next;
      });
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
    onSuccess: (_resp, name) => {
      // Mirror the optimism: pull it out of the cache immediately.
      qc.setQueryData<WasmFunction[]>(qk.wasm.list(), (prev) =>
        (prev ?? []).filter((f) => f.name !== name),
      );
      qc.invalidateQueries({ queryKey: qk.wasm.list() });
    },
  });
}
