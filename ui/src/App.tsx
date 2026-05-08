import { Outlet, useLocation } from 'react-router-dom';
import { ConnectionBar } from './components/layout/ConnectionBar';
import { Sidebar } from './components/layout/Sidebar';
import { ToastViewport } from './components/shared/ToastViewport';
import { DialogHost } from './components/shared/Dialog';
import { useConfigBootstrap } from './lib/connection-store';
import { useClusterSnapshotStream, useFunctionsStream } from './hooks/useStreams';

export default function App() {
  const location = useLocation();
  useConfigBootstrap();
  // Subscribe once at the app root to the server-streaming feeds.
  // The streams write into the TanStack Query cache, so view-level
  // hooks (`useClusterSnapshot`, `useWasmFunctions`) automatically
  // see live updates with no polling overhead.
  useClusterSnapshotStream();
  useFunctionsStream();
  return (
    <div className="flex flex-col h-screen w-screen bg-bg-0 text-text-primary">
      <ConnectionBar />
      <div className="flex flex-1 overflow-hidden">
        <Sidebar pathname={location.pathname} />
        <main className="flex-1 overflow-hidden flex flex-col">
          <Outlet />
        </main>
      </div>
      <ToastViewport />
      <DialogHost />
    </div>
  );
}
