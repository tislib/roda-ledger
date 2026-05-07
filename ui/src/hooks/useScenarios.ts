import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useClusterClient } from '@/lib/cluster-client.runtime';
import { qk } from '@/lib/query-keys';
import type { Scenario } from '@/types/scenario';

export function useScenarioStatus(runId: string | null) {
  const client = useClusterClient();
  return useQuery({
    queryKey: runId ? qk.scenarios.status(runId) : ['scenarios', 'status', null],
    queryFn: () => (runId ? client.getScenarioStatus(runId) : Promise.reject('no run id')),
    enabled: runId != null,
    refetchInterval: (query) => {
      const data = query.state.data;
      if (!data) return 500;
      return data.state === 'Running' || data.state === 'Queued' ? 500 : false;
    },
  });
}

export function useScenarioRuns(limit = 32, intervalMs = 2000) {
  const client = useClusterClient();
  return useQuery({
    queryKey: qk.scenarios.runs(),
    queryFn: () => client.listScenarioRuns(limit),
    refetchInterval: intervalMs,
    placeholderData: (prev) => prev,
  });
}

export function useRunScenario() {
  const client = useClusterClient();
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (scenario: Scenario) => client.runScenario(scenario),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: qk.scenarios.runs() });
    },
  });
}

export function useCancelScenario() {
  const client = useClusterClient();
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (runId: string) => client.cancelScenario(runId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: qk.scenarios.runs() });
    },
  });
}
