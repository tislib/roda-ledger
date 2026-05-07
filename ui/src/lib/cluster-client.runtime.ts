import { createContext, useContext } from 'react';
import type { ClusterClient } from './cluster-client';

export const ClusterClientContext = createContext<ClusterClient | null>(null);

export function useClusterClient(): ClusterClient {
  const client = useContext(ClusterClientContext);
  if (!client) {
    throw new Error('useClusterClient must be used within ClusterClientProvider');
  }
  return client;
}
