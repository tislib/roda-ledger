import { useEffect, useRef, useState } from 'react';
import { useConnections } from '@/lib/connection-store';
import { useServerInfo } from '@/hooks/useClusterSnapshot';
import { cn } from '@/lib/cn';

export function ConnectionBar() {
  const { connections, active, setActive, add, update, remove } = useConnections();
  const serverInfo = useServerInfo();
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(active?.url ?? '');
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setDraft(active?.url ?? '');
  }, [active?.url]);

  useEffect(() => {
    if (!menuOpen) return;
    const onClickOutside = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', onClickOutside);
    return () => document.removeEventListener('mousedown', onClickOutside);
  }, [menuOpen]);

  const connectionState: 'connecting' | 'connected' | 'error' = serverInfo.isLoading
    ? 'connecting'
    : serverInfo.isError
      ? 'error'
      : 'connected';

  const stateColor = {
    connecting: 'bg-accent animate-pulse',
    connected: 'bg-health-up',
    error: 'bg-health-crashed',
  }[connectionState];

  const stateLabel = {
    connecting: 'connecting',
    connected: serverInfo.data ? `connected · ${serverInfo.data.version}` : 'connected',
    error: 'error',
  }[connectionState];

  const onSave = () => {
    if (active && draft.trim()) {
      update(active.id, { url: draft.trim() });
    }
    setEditing(false);
  };

  return (
    <div className="h-10 bg-bg-1 border-b border-border-subtle flex items-center px-3 gap-2 text-xs">
      <div className="flex items-center gap-2 mr-2">
        <span className="font-mono font-semibold tracking-tight text-text-primary">roda</span>
      </div>

      <div className={cn('flex items-center gap-1.5 px-2 py-0.5 rounded text-[10px] font-mono', editing && 'opacity-50')}>
        <span className={cn('inline-block w-1.5 h-1.5 rounded-full', stateColor)} />
        <span className="text-text-muted">{stateLabel}</span>
      </div>

      <div className="flex-1 flex items-center gap-1 max-w-2xl">
        {editing ? (
          <>
            <input
              autoFocus
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') onSave();
                if (e.key === 'Escape') {
                  setDraft(active?.url ?? '');
                  setEditing(false);
                }
              }}
              className="flex-1 bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono text-text-primary
                        focus:outline-none focus:border-accent/60"
              placeholder="http://host:port"
            />
            <button onClick={onSave} className="btn btn-accent">Save</button>
            <button onClick={() => { setDraft(active?.url ?? ''); setEditing(false); }} className="btn">Cancel</button>
          </>
        ) : (
          <>
            <button
              onClick={() => setEditing(true)}
              className="flex-1 text-left bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono text-text-secondary
                        hover:border-border-strong hover:text-text-primary transition-colors"
              title="Click to edit"
            >
              {active?.url ?? '(no connection)'}
            </button>
          </>
        )}
      </div>

      <div ref={menuRef} className="relative">
        <button onClick={() => setMenuOpen((v) => !v)} className="btn">
          {active?.label ?? '—'}
          <span className="text-text-muted">▾</span>
        </button>

        {menuOpen && (
          <div className="absolute right-0 top-full mt-1 w-72 pane py-1 z-50">
            <div className="px-3 py-1 label">Saved connections</div>
            <ul className="max-h-64 overflow-auto">
              {connections.map((c) => (
                <li
                  key={c.id}
                  className={cn(
                    'flex items-center justify-between px-3 py-1.5 text-xs hover:bg-bg-2 cursor-pointer',
                    active?.id === c.id && 'bg-bg-2',
                  )}
                  onClick={() => {
                    setActive(c.id);
                    setMenuOpen(false);
                  }}
                >
                  <div className="flex flex-col min-w-0">
                    <span className="text-text-primary truncate">{c.label}</span>
                    <span className="font-mono text-[10px] text-text-muted truncate">{c.url}</span>
                  </div>
                  {connections.length > 1 && (
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        if (confirm(`Remove "${c.label}"?`)) remove(c.id);
                      }}
                      className="text-text-muted hover:text-health-crashed text-[10px] px-1"
                      title="Remove"
                    >
                      ×
                    </button>
                  )}
                </li>
              ))}
            </ul>
            <div className="border-t border-border-subtle mt-1 pt-1">
              <button
                onClick={() => {
                  const label = prompt('Connection label:');
                  if (label == null) return;
                  const url = prompt('Control plane URL:', 'http://localhost:50051');
                  if (url == null || !url.trim()) return;
                  add(label, url);
                  setMenuOpen(false);
                }}
                className="w-full text-left px-3 py-1.5 text-xs text-text-secondary hover:bg-bg-2 hover:text-text-primary"
              >
                + Add connection
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
