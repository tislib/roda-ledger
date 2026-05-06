import { createBrowserRouter, Navigate } from 'react-router-dom';
import App from './App';
import { ClusterDashboard } from './components/cluster/ClusterDashboard';
import { ClusterLogs } from './components/cluster/ClusterLogs';
import { ClusterSettings } from './components/cluster/ClusterSettings';
import { ClusterFaults } from './components/cluster/ClusterFaults';
import { LedgerModule } from './components/ledger/LedgerModule';
import { MetaModule } from './components/meta/MetaModule';
import { LoadModule } from './components/load/LoadModule';

export const router = createBrowserRouter([
  {
    path: '/',
    element: <App />,
    children: [
      { index: true, element: <Navigate to="/cluster/dashboard" replace /> },

      // Cluster module
      { path: 'cluster', element: <Navigate to="/cluster/dashboard" replace /> },
      { path: 'cluster/dashboard', element: <ClusterDashboard /> },
      { path: 'cluster/logs', element: <ClusterLogs /> },
      { path: 'cluster/logs/:nodeId', element: <ClusterLogs /> },
      { path: 'cluster/settings', element: <ClusterSettings /> },
      { path: 'cluster/faults', element: <ClusterFaults /> },

      // Other modules
      { path: 'ledger', element: <LedgerModule /> },
      { path: 'meta', element: <MetaModule /> },
      { path: 'load', element: <LoadModule /> },
    ],
  },
]);
