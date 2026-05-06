import React from 'react';
import ReactDOM from 'react-dom/client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RouterProvider } from 'react-router-dom';

import { router } from './routes';
import { ClusterClientProvider } from './lib/cluster-client.context';
import { MockClusterClient } from './mocks/MockClusterClient';
import './styles/globals.css';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 0,
      refetchOnWindowFocus: false,
      retry: false,
    },
  },
});

const client = new MockClusterClient();

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <ClusterClientProvider client={client}>
        <RouterProvider router={router} />
      </ClusterClientProvider>
    </QueryClientProvider>
  </React.StrictMode>,
);
