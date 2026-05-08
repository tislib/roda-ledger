import { useEffect } from 'react';
import { useQueryClient } from '@tanstack/react-query';

import { useClusterClient } from '@/lib/cluster-client.runtime';
import { qk } from '@/lib/query-keys';

/**
 * Drives `watchClusterSnapshot` (server-streaming) into the TanStack
 * Query cache under the `cluster.snapshot` key. Existing consumers
 * (`useClusterSnapshot`) keep working without changes — they just see
 * data appear faster, with no client-side polling.
 *
 * Falls back silently if the active client doesn't support streaming
 * (e.g. `mock://local` — those keep polling).
 *
 * Reconnects with a 1s backoff on stream error / drop.
 */
export function useClusterSnapshotStream() {
  const client = useClusterClient();
  const qc = useQueryClient();

  useEffect(() => {
    if (!client.watchClusterSnapshot) return;
    let cancelled = false;
    let reconnect: ReturnType<typeof setTimeout> | null = null;

    const loop = async () => {
      while (!cancelled && client.watchClusterSnapshot) {
        try {
          for await (const snap of client.watchClusterSnapshot(250)) {
            if (cancelled) return;
            qc.setQueryData(qk.cluster.snapshot(), snap);
          }
        } catch {
          // stream error — fall through to reconnect
        }
        if (cancelled) return;
        await new Promise<void>((r) => {
          reconnect = setTimeout(r, 1_000);
        });
      }
    };

    loop();
    return () => {
      cancelled = true;
      if (reconnect) clearTimeout(reconnect);
    };
  }, [client, qc]);
}

/**
 * Drives `watchFunctions` into the cache. Eliminates the deploy-flicker
 * the Meta module saw with polling — server only emits on actual change.
 */
export function useFunctionsStream() {
  const client = useClusterClient();
  const qc = useQueryClient();

  useEffect(() => {
    if (!client.watchFunctions) return;
    let cancelled = false;
    let reconnect: ReturnType<typeof setTimeout> | null = null;

    const loop = async () => {
      while (!cancelled && client.watchFunctions) {
        try {
          for await (const fns of client.watchFunctions(1_000)) {
            if (cancelled) return;
            qc.setQueryData(qk.wasm.list(), fns);
          }
        } catch {
          // fall through to reconnect
        }
        if (cancelled) return;
        await new Promise<void>((r) => {
          reconnect = setTimeout(r, 1_000);
        });
      }
    };

    loop();
    return () => {
      cancelled = true;
      if (reconnect) clearTimeout(reconnect);
    };
  }, [client, qc]);
}
