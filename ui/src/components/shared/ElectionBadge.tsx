import { AnimatePresence, motion } from 'framer-motion';
import { useEffect, useState } from 'react';
import type { ElectionEvent } from '@/types/cluster';

interface Props {
  events: ElectionEvent[];
  /** Show duration in ms. */
  durationMs?: number;
}

export function ElectionBadge({ events, durationMs = 3000 }: Props) {
  const latest = events[0] ?? null;
  const [visible, setVisible] = useState<ElectionEvent | null>(null);

  useEffect(() => {
    if (!latest) return;
    setVisible(latest);
    const t = setTimeout(() => setVisible(null), durationMs);
    return () => clearTimeout(t);
  }, [latest, durationMs]);

  return (
    <AnimatePresence>
      {visible && (
        <motion.div
          role="status"
          aria-live="polite"
          aria-atomic
          initial={{ opacity: 0, y: -8 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -8 }}
          transition={{ duration: 0.18 }}
          // z-[60] sits above the connection menu (z-50) so the badge is
          // never occluded mid-demo.
          className="fixed top-14 right-4 z-[60] pane px-3 py-2 flex items-center gap-2 text-xs shadow-lg"
        >
          <span className="w-1.5 h-1.5 rounded-full bg-accent animate-pulse" />
          <span className="text-text-muted">election complete</span>
          <span className="font-mono text-text-primary">
            term&nbsp;{visible.term}
          </span>
          <span className="text-text-muted">→</span>
          <span className="font-mono text-role-leader">
            n{visible.winnerNodeId ?? '?'}
          </span>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
