import type { ReactNode } from 'react';
import type { ClusterClient } from './cluster-client';
import { ClusterClientContext } from './cluster-client.runtime';

interface ProviderProps {
  client: ClusterClient;
  children: ReactNode;
}

export function ClusterClientProvider({ client, children }: ProviderProps) {
  return (
    <ClusterClientContext.Provider value={client}>{children}</ClusterClientContext.Provider>
  );
}
