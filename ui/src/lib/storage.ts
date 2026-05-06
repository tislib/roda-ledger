/** Versioned localStorage helpers. Centralized so we can clear/migrate consistently. */

const NAMESPACE = 'roda.ui.v1';

function key(name: string): string {
  return `${NAMESPACE}.${name}`;
}

export function loadJson<T>(name: string, fallback: T): T {
  try {
    const raw = window.localStorage.getItem(key(name));
    if (raw == null) return fallback;
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

export function saveJson<T>(name: string, value: T): void {
  try {
    window.localStorage.setItem(key(name), JSON.stringify(value));
  } catch {
    // localStorage may be unavailable / quota exceeded — ignore.
  }
}

export function removeKey(name: string): void {
  try {
    window.localStorage.removeItem(key(name));
  } catch {
    // ignore
  }
}

export const STORAGE_KEYS = {
  connections: 'connections',
  activeConnectionId: 'activeConnectionId',
  scenarios: 'scenarios',
} as const;
