/**
 * Imperative dialog API. Component-side rendering lives in
 * `components/shared/Dialog.tsx`; this module is the store + the public
 * `dialog.confirm()` / `dialog.prompt()` helpers.
 */
import type { ReactNode } from 'react';
import { useSyncExternalStore } from 'react';

export interface ConfirmRequest {
  id: number;
  title: string;
  description?: ReactNode;
  confirmLabel?: string;
  cancelLabel?: string;
  destructive?: boolean;
  resolve: (ok: boolean) => void;
}

export interface PromptField {
  name: string;
  label: string;
  placeholder?: string;
  initial?: string;
  required?: boolean;
}

export interface PromptRequest {
  id: number;
  title: string;
  description?: ReactNode;
  fields: PromptField[];
  confirmLabel?: string;
  resolve: (values: Record<string, string> | null) => void;
}

type Listener = () => void;

interface DialogSnapshot {
  confirm: ConfirmRequest | null;
  prompt: PromptRequest | null;
}

class DialogStore {
  private confirm_: ConfirmRequest | null = null;
  private prompt_: PromptRequest | null = null;
  private seq = 1;
  private listeners = new Set<Listener>();
  // Cached snapshot — `useSyncExternalStore` requires referential stability
  // when the underlying state hasn't changed. Returning a fresh object
  // literal each call makes React re-render every commit and infinite-loop.
  private snap: DialogSnapshot = { confirm: null, prompt: null };

  getSnapshot = (): DialogSnapshot => this.snap;

  subscribe = (cb: Listener): (() => void) => {
    this.listeners.add(cb);
    return () => {
      this.listeners.delete(cb);
    };
  };

  confirm(req: Omit<ConfirmRequest, 'id' | 'resolve'>): Promise<boolean> {
    return new Promise((resolve) => {
      this.confirm_ = { ...req, id: this.seq++, resolve };
      this.refreshSnap();
      this.emit();
    });
  }

  resolveConfirm(ok: boolean): void {
    const r = this.confirm_;
    this.confirm_ = null;
    this.refreshSnap();
    this.emit();
    r?.resolve(ok);
  }

  prompt(req: Omit<PromptRequest, 'id' | 'resolve'>): Promise<Record<string, string> | null> {
    return new Promise((resolve) => {
      this.prompt_ = { ...req, id: this.seq++, resolve };
      this.refreshSnap();
      this.emit();
    });
  }

  resolvePrompt(values: Record<string, string> | null): void {
    const r = this.prompt_;
    this.prompt_ = null;
    this.refreshSnap();
    this.emit();
    r?.resolve(values);
  }

  private refreshSnap(): void {
    this.snap = { confirm: this.confirm_, prompt: this.prompt_ };
  }

  private emit(): void {
    for (const cb of this.listeners) cb();
  }
}

export const dialogStore = new DialogStore();

export const dialog = {
  confirm: (opts: Omit<ConfirmRequest, 'id' | 'resolve'>) => dialogStore.confirm(opts),
  prompt: (opts: Omit<PromptRequest, 'id' | 'resolve'>) => dialogStore.prompt(opts),
};

export function useDialogState() {
  return useSyncExternalStore(dialogStore.subscribe, dialogStore.getSnapshot);
}
