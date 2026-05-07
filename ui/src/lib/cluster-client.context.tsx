import { useEffect, useRef, type ReactNode } from 'react';
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

// ---------------------------------------------------------------------------
// Module-level singleton.
//
// The client's lifecycle is intentionally decoupled from React's component
// lifecycle. React StrictMode's simulated mount → unmount → remount cycle
// would otherwise either (a) make us rebuild the simulator twice on every
// page load, or (b) leave us with a disposed simulator if we tried to be
// clever about it. Holding the client at module scope and keying it by URL
// sidesteps the whole class of issues — render is pure, no setState in
// cleanups, and HMR / route remounts don't churn the simulator's interval.
// ---------------------------------------------------------------------------
let currentUrl: string | null | undefined = undefined;
let currentClient: DisposableClusterClient | null = null;

function getClientForUrl(url: string | null): DisposableClusterClient | null {
  if (currentUrl === url && currentClient) return currentClient;
  if (currentClient && typeof currentClient.dispose === 'function') {
    currentClient.dispose();
  }
  currentUrl = url;
  currentClient = buildClient(url);
  return currentClient;
}

// Vite HMR: dispose on module replacement so the simulator's setInterval
// doesn't accumulate across hot reloads.
if (import.meta.hot) {
  import.meta.hot.dispose(() => {
    if (currentClient && typeof currentClient.dispose === 'function') {
      currentClient.dispose();
    }
    currentClient = null;
    currentUrl = undefined;
  });
}

/**
 * Provider exposes the singleton client to React. Clears all query caches
 * once per genuine URL change (StrictMode-safe via a ref guard).
 */
export function ClusterClientProvider({ children }: { children: ReactNode }) {
  const { active } = useConnections();
  const url = active?.url ?? null;
  const qc = useQueryClient();

  const client = getClientForUrl(url);

  const lastClearedRef = useRef<string | null | undefined>(undefined);
  useEffect(() => {
    if (lastClearedRef.current === undefined) {
      // First run: nothing to clear. Just record the URL.
      lastClearedRef.current = url;
      return;
    }
    if (lastClearedRef.current !== url) {
      lastClearedRef.current = url;
      qc.clear();
    }
  }, [url, qc]);

  if (!client) return null;
  return (
    <ClusterClientContext.Provider value={client}>{children}</ClusterClientContext.Provider>
  );
}
