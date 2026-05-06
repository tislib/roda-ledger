import { AnimatePresence, motion } from 'framer-motion';
import { cn } from '@/lib/cn';

interface Props {
  term: string;
  className?: string;
}

export function TermPill({ term, className }: Props) {
  return (
    <span
      className={cn(
        'inline-flex items-center gap-0.5 font-mono text-[11px] text-text-secondary',
        className,
      )}
    >
      <span className="text-text-muted">term</span>
      <span className="relative inline-block min-w-[1.5ch] text-center">
        <AnimatePresence mode="popLayout">
          <motion.span
            key={term}
            initial={{ y: -8, opacity: 0 }}
            animate={{ y: 0, opacity: 1 }}
            exit={{ y: 8, opacity: 0 }}
            transition={{ duration: 0.18, ease: 'easeOut' }}
            className="text-text-primary inline-block"
          >
            {term}
          </motion.span>
        </AnimatePresence>
      </span>
    </span>
  );
}
