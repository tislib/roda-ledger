/**
 * Tiny toast queue, useSyncExternalStore-backed. No deps.
 * Render <ToastViewport /> once at the app root.
 */
import { useSyncExternalStore } from 'react';

export type ToastTone = 'info' | 'success' | 'warning' | 'error';

export interface Toast {
  id: string;
  tone: ToastTone;
  title: string;
  description?: string;
  /** Auto-dismiss after this many ms; 0 = sticky. */
  durationMs: number;
}

type Listener = () => void;

class ToastStore {
  private items: Toast[] = [];
  private listeners = new Set<Listener>();

  getSnapshot = (): readonly Toast[] => this.items;

  subscribe = (cb: Listener): (() => void) => {
    this.listeners.add(cb);
    return () => {
      this.listeners.delete(cb);
    };
  };

  push(tone: ToastTone, title: string, description?: string, durationMs = 4000): string {
    const id = `t_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
    const toast: Toast = { id, tone, title, description, durationMs };
    this.items = [...this.items, toast];
    this.emit();
    if (durationMs > 0) {
      setTimeout(() => this.dismiss(id), durationMs);
    }
    return id;
  }

  dismiss(id: string): void {
    const before = this.items.length;
    this.items = this.items.filter((t) => t.id !== id);
    if (this.items.length !== before) this.emit();
  }

  clear(): void {
    if (this.items.length === 0) return;
    this.items = [];
    this.emit();
  }

  private emit(): void {
    for (const cb of this.listeners) cb();
  }
}

export const toastStore = new ToastStore();

export const toast = {
  info: (title: string, description?: string) => toastStore.push('info', title, description),
  success: (title: string, description?: string) =>
    toastStore.push('success', title, description),
  warning: (title: string, description?: string) =>
    toastStore.push('warning', title, description, 6000),
  error: (title: string, description?: string) =>
    toastStore.push('error', title, description, 8000),
  dismiss: (id: string) => toastStore.dismiss(id),
  clear: () => toastStore.clear(),
};

export function useToasts(): readonly Toast[] {
  return useSyncExternalStore(toastStore.subscribe, toastStore.getSnapshot);
}
