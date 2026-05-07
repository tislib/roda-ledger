export interface SavedConnection {
  /** Stable id (uuid-ish, generated client-side). */
  id: string;
  /** User-provided label, e.g. "Local mock", "staging-1". */
  label: string;
  /** Control plane base URL. */
  url: string;
  /** Wall-clock when last selected. */
  lastUsedAt: number;
}

export type ConnectionState = 'connecting' | 'connected' | 'error';
