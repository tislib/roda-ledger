export const qk = {
  server: {
    info: () => ['server', 'info'] as const,
  },
  cluster: {
    all: ['cluster'] as const,
    snapshot: () => ['cluster', 'snapshot'] as const,
    elections: () => ['cluster', 'elections'] as const,
    log: (nodeId: string, fromIndex?: string) =>
      ['cluster', 'log', nodeId, fromIndex ?? '0'] as const,
    config: () => ['cluster', 'config'] as const,
    faults: () => ['cluster', 'faults'] as const,
  },
  tx: {
    all: ['tx'] as const,
    status: (txId: string) => ['tx', 'status', txId] as const,
  },
  wasm: {
    all: ['wasm'] as const,
    list: () => ['wasm', 'list'] as const,
    examples: () => ['wasm', 'examples'] as const,
  },
  scenarios: {
    runs: () => ['scenarios', 'runs'] as const,
    status: (runId: string) => ['scenarios', 'status', runId] as const,
  },
} as const;
