import { AnimatePresence, motion } from 'framer-motion';
import { useToasts, toast as toastApi, type ToastTone } from '@/lib/toast';
import { cn } from '@/lib/cn';

const TONE_STYLES: Record<ToastTone, string> = {
  info: 'border-border bg-bg-1',
  success: 'border-health-up/40 bg-health-up/10',
  warning: 'border-accent/40 bg-accent/10',
  error: 'border-health-crashed/40 bg-health-crashed/10',
};

const TONE_DOT: Record<ToastTone, string> = {
  info: 'bg-text-secondary',
  success: 'bg-health-up',
  warning: 'bg-accent',
  error: 'bg-health-crashed',
};

export function ToastViewport() {
  const toasts = useToasts();
  return (
    <div
      className="fixed bottom-4 right-4 z-[60] flex flex-col gap-2 w-80 pointer-events-none"
      aria-live="polite"
    >
      <AnimatePresence>
        {toasts.map((t) => (
          <motion.div
            key={t.id}
            initial={{ opacity: 0, y: 20, scale: 0.95 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 20, scale: 0.95 }}
            transition={{ duration: 0.18, ease: 'easeOut' }}
            className={cn(
              'pane border px-3 py-2 text-xs shadow-lg pointer-events-auto',
              TONE_STYLES[t.tone],
            )}
            role={t.tone === 'error' ? 'alert' : 'status'}
          >
            <div className="flex items-start gap-2">
              <span className={cn('inline-block w-1.5 h-1.5 rounded-full mt-1.5 shrink-0', TONE_DOT[t.tone])} />
              <div className="flex-1 min-w-0">
                <div className="font-medium text-text-primary">{t.title}</div>
                {t.description && (
                  <div className="text-text-muted mt-0.5 break-words">{t.description}</div>
                )}
              </div>
              <button
                onClick={() => toastApi.dismiss(t.id)}
                className="text-text-muted hover:text-text-primary text-[14px] leading-none"
                aria-label="Dismiss"
              >
                ×
              </button>
            </div>
          </motion.div>
        ))}
      </AnimatePresence>
    </div>
  );
}
