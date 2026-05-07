import { useEffect, useRef, type FormEvent, type ReactNode } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { cn } from '@/lib/cn';
import {
  dialogStore,
  useDialogState,
  type ConfirmRequest,
  type PromptRequest,
} from '@/lib/dialog';

interface DialogProps {
  open: boolean;
  onClose: () => void;
  title: string;
  description?: ReactNode;
  children?: ReactNode;
  /** Default footer if `children` doesn't render its own. */
  footer?: ReactNode;
  className?: string;
  /** When false, clicking the backdrop or pressing Escape does not close. */
  dismissable?: boolean;
}

/**
 * Lightweight modal. Renders a fixed-position overlay over the whole viewport.
 * Escape closes; first focusable element receives focus on open.
 */
export function Dialog({
  open,
  onClose,
  title,
  description,
  children,
  footer,
  className,
  dismissable = true,
}: DialogProps) {
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && dismissable) onClose();
    };
    document.addEventListener('keydown', onKey);
    const t = setTimeout(() => {
      panelRef.current
        ?.querySelector<HTMLElement>('input,textarea,select,button,[tabindex]:not([tabindex="-1"])')
        ?.focus();
    }, 0);
    return () => {
      document.removeEventListener('keydown', onKey);
      clearTimeout(t);
    };
  }, [open, onClose, dismissable]);

  return (
    <AnimatePresence>
      {open && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.12 }}
          className="fixed inset-0 z-[55] bg-black/60 flex items-center justify-center p-4"
          onMouseDown={(e) => {
            if (dismissable && e.target === e.currentTarget) onClose();
          }}
          role="dialog"
          aria-modal
          aria-labelledby="dialog-title"
        >
          <motion.div
            ref={panelRef}
            initial={{ opacity: 0, scale: 0.96, y: 8 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.96, y: 8 }}
            transition={{ duration: 0.14, ease: 'easeOut' }}
            className={cn('pane w-full max-w-md p-4 space-y-3 shadow-2xl', className)}
          >
            <div>
              <h2 id="dialog-title" className="text-sm font-semibold text-text-primary">
                {title}
              </h2>
              {description && (
                <div className="text-xs text-text-muted mt-1">{description}</div>
              )}
            </div>
            {children}
            {footer && <div className="flex items-center justify-end gap-2 pt-2">{footer}</div>}
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}

export function DialogHost() {
  const state = useDialogState();
  return (
    <>
      {state.confirm && (
        <ConfirmDialog
          key={state.confirm.id}
          req={state.confirm}
          onResolve={(ok) => dialogStore.resolveConfirm(ok)}
        />
      )}
      {state.prompt && (
        <PromptDialog
          key={state.prompt.id}
          req={state.prompt}
          onResolve={(v) => dialogStore.resolvePrompt(v)}
        />
      )}
    </>
  );
}

function ConfirmDialog({
  req,
  onResolve,
}: {
  req: ConfirmRequest;
  onResolve: (ok: boolean) => void;
}) {
  return (
    <Dialog
      open
      onClose={() => onResolve(false)}
      title={req.title}
      description={req.description}
      footer={
        <>
          <button onClick={() => onResolve(false)} className="btn">
            {req.cancelLabel ?? 'Cancel'}
          </button>
          <button
            onClick={() => onResolve(true)}
            className={cn('btn', req.destructive ? 'btn-danger' : 'btn-accent')}
            autoFocus
          >
            {req.confirmLabel ?? 'OK'}
          </button>
        </>
      }
    />
  );
}

function PromptDialog({
  req,
  onResolve,
}: {
  req: PromptRequest;
  onResolve: (values: Record<string, string> | null) => void;
}) {
  const onSubmit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const fd = new FormData(e.currentTarget);
    const values: Record<string, string> = {};
    for (const f of req.fields) {
      const v = String(fd.get(f.name) ?? '');
      if (f.required && !v.trim()) return;
      values[f.name] = v;
    }
    onResolve(values);
  };
  return (
    <Dialog
      open
      onClose={() => onResolve(null)}
      title={req.title}
      description={req.description}
    >
      <form onSubmit={onSubmit} className="space-y-3">
        {req.fields.map((f) => (
          <div key={f.name} className="flex flex-col gap-1">
            <label className="text-[10px] font-mono text-text-muted">{f.label}</label>
            <input
              name={f.name}
              defaultValue={f.initial ?? ''}
              placeholder={f.placeholder}
              required={f.required}
              className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono text-text-primary
                        focus:outline-none focus:border-accent/60"
            />
          </div>
        ))}
        <div className="flex items-center justify-end gap-2 pt-2">
          <button type="button" onClick={() => onResolve(null)} className="btn">
            Cancel
          </button>
          <button type="submit" className="btn btn-accent">
            {req.confirmLabel ?? 'Save'}
          </button>
        </div>
      </form>
    </Dialog>
  );
}
