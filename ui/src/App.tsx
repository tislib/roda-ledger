import { Outlet, useLocation } from 'react-router-dom';
import { ConnectionBar } from './components/layout/ConnectionBar';
import { Sidebar } from './components/layout/Sidebar';
import { ToastViewport } from './components/shared/ToastViewport';
import { DialogHost } from './components/shared/Dialog';
import { useConfigBootstrap } from './lib/connection-store';

export default function App() {
  const location = useLocation();
  useConfigBootstrap();
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
