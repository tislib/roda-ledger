import { useEffect, useMemo, useRef, type ReactNode } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import type { ClusterClient } from './cluster-client';
import { ClusterClientContext } from './cluster-client.runtime';
import { MockClusterClient } from '@/mocks/MockClusterClient';
import { RealClusterClient } from './real-cluster-client';
import { useConnections } from './connection-store';

interface DisposableClusterClient extends ClusterClient {
  dispose?(): void;
}

function buildClient(url: string | null): DisposableClusterClient | null {
  if (!url) return null;
  if (url.startsWith('mock://')) {
    return new MockClusterClient();
  }
  if (url.startsWith('http://') || url.startsWith('https://')) {
    return new RealClusterClient(url);
  }
  // Unknown scheme — fall back to mock for safety in V1.
  return new MockClusterClient();
}

/**
 * Builds a `ClusterClient` from the active connection's URL and disposes the
 * previous one when the URL changes. All TanStack Query caches are cleared on
 * switch so the new cluster's data isn't shadowed by the old one.
 */
export function ClusterClientProvider({ children }: { children: ReactNode }) {
  const { active } = useConnections();
  const url = active?.url ?? null;
  const qc = useQueryClient();
  const prev = useRef<DisposableClusterClient | null>(null);

  const client = useMemo(() => {
    // Dispose previous client (mock simulator's setInterval, etc.).
    if (prev.current && typeof prev.current.dispose === 'function') {
      prev.current.dispose();
    }
    const next = buildClient(url);
    prev.current = next;
    return next;
  }, [url]);

  useEffect(() => {
    qc.clear();
  }, [url, qc]);

  // Final cleanup on unmount.
  useEffect(() => {
    return () => {
      if (prev.current && typeof prev.current.dispose === 'function') {
        prev.current.dispose();
      }
      prev.current = null;
    };
  }, []);

  if (!client) return null;

  return (
    <ClusterClientContext.Provider value={client}>{children}</ClusterClientContext.Provider>
  );
}
