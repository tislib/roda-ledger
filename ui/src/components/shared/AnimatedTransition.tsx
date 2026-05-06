import { motion, AnimatePresence } from 'framer-motion';
import type { ReactNode } from 'react';

interface Props {
  /** Key value that triggers the transition when it changes. */
  flashKey: string | number;
  children: ReactNode;
  className?: string;
}

export function AnimatedTransition({ flashKey, children, className }: Props) {
  return (
    <AnimatePresence mode="popLayout">
      <motion.div
        key={flashKey}
        initial={{ backgroundColor: 'rgba(245, 165, 36, 0.25)' }}
        animate={{ backgroundColor: 'rgba(245, 165, 36, 0)' }}
        transition={{ duration: 0.8, ease: 'easeOut' }}
        className={className}
      >
        {children}
      </motion.div>
    </AnimatePresence>
  );
}
