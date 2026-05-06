import { useEffect, useSyncExternalStore } from 'react';
import type { SavedConnection } from '@/types/connection';
import { loadJson, saveJson, STORAGE_KEYS } from './storage';

const DEFAULT_URL = 'http://localhost:50051';

interface ConnectionState {
  connections: SavedConnection[];
  activeId: string | null;
}

function generateId(): string {
  return `conn_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

class ConnectionStore {
  private state: ConnectionState;
  private listeners = new Set<() => void>();

  constructor() {
    const connections = loadJson<SavedConnection[]>(STORAGE_KEYS.connections, []);
    const activeId = loadJson<string | null>(STORAGE_KEYS.activeConnectionId, null);

    if (connections.length === 0) {
      const seed: SavedConnection = {
        id: generateId(),
        label: 'Local mock',
        url: DEFAULT_URL,
        lastUsedAt: Date.now(),
      };
      this.state = { connections: [seed], activeId: seed.id };
      this.persist();
    } else {
      this.state = {
        connections,
        activeId: activeId && connections.some((c) => c.id === activeId) ? activeId : connections[0]!.id,
      };
    }
  }

  /** Optionally seed the active URL from a config.json shipped with the build. */
  hydrateFromConfig(configUrl: string): void {
    if (!configUrl) return;
    const exists = this.state.connections.find((c) => c.url === configUrl);
    if (exists) {
      this.state = { ...this.state, activeId: exists.id };
    } else {
      const conn: SavedConnection = {
        id: generateId(),
        label: 'Default (config.json)',
        url: configUrl,
        lastUsedAt: Date.now(),
      };
      this.state = { connections: [conn, ...this.state.connections], activeId: conn.id };
    }
    this.persist();
    this.emit();
  }

  getSnapshot = (): ConnectionState => this.state;

  subscribe = (cb: () => void): (() => void) => {
    this.listeners.add(cb);
    return () => {
      this.listeners.delete(cb);
    };
  };

  active(): SavedConnection | null {
    return this.state.connections.find((c) => c.id === this.state.activeId) ?? null;
  }

  setActive(id: string): void {
    if (!this.state.connections.some((c) => c.id === id)) return;
    const connections = this.state.connections.map((c) =>
      c.id === id ? { ...c, lastUsedAt: Date.now() } : c,
    );
    this.state = { connections, activeId: id };
    this.persist();
    this.emit();
  }

  add(label: string, url: string): SavedConnection {
    const conn: SavedConnection = {
      id: generateId(),
      label: label.trim() || url,
      url: url.trim(),
      lastUsedAt: Date.now(),
    };
    this.state = {
      connections: [...this.state.connections, conn],
      activeId: conn.id,
    };
    this.persist();
    this.emit();
    return conn;
  }

  update(id: string, patch: Partial<Omit<SavedConnection, 'id'>>): void {
    this.state = {
      ...this.state,
      connections: this.state.connections.map((c) =>
        c.id === id ? { ...c, ...patch } : c,
      ),
    };
    this.persist();
    this.emit();
  }

  remove(id: string): void {
    if (this.state.connections.length <= 1) return; // keep at least one
    const connections = this.state.connections.filter((c) => c.id !== id);
    const activeId = this.state.activeId === id ? connections[0]!.id : this.state.activeId;
    this.state = { connections, activeId };
    this.persist();
    this.emit();
  }

  private persist(): void {
    saveJson(STORAGE_KEYS.connections, this.state.connections);
    saveJson(STORAGE_KEYS.activeConnectionId, this.state.activeId);
  }

  private emit(): void {
    for (const cb of this.listeners) cb();
  }
}

export const connectionStore = new ConnectionStore();

export function useConnections() {
  const state = useSyncExternalStore(connectionStore.subscribe, connectionStore.getSnapshot);
  return {
    connections: state.connections,
    active: state.connections.find((c) => c.id === state.activeId) ?? null,
    setActive: (id: string) => connectionStore.setActive(id),
    add: (label: string, url: string) => connectionStore.add(label, url),
    update: (id: string, patch: Partial<Omit<SavedConnection, 'id'>>) =>
      connectionStore.update(id, patch),
    remove: (id: string) => connectionStore.remove(id),
  };
}

/**
 * Fetches /config.json on first mount and seeds the default connection if
 * the file exists and exposes a `controlPlaneUrl`.
 */
export function useConfigBootstrap() {
  useEffect(() => {
    let cancelled = false;
    fetch('/config.json', { method: 'GET' })
      .then((r) => (r.ok ? r.json() : null))
      .then((cfg: unknown) => {
        if (cancelled || !cfg || typeof cfg !== 'object') return;
        const url = (cfg as { controlPlaneUrl?: string }).controlPlaneUrl;
        if (typeof url === 'string' && url.length > 0) {
          connectionStore.hydrateFromConfig(url);
        }
      })
      .catch(() => {
        // /config.json missing — fine, fall back to localStorage / default.
      });
    return () => {
      cancelled = true;
    };
  }, []);
}
