import { cn } from '@/lib/cn';

interface Props {
  className?: string;
}

/**
 * Subtle shimmer block. Sized by parent via `className`. Use as a
 * size-matched placeholder for the eventual real layout so first-paint
 * doesn't reflow when data lands.
 */
export function Skeleton({ className }: Props) {
  return (
    <div
      className={cn(
        'animate-pulse-slow bg-bg-2 rounded border border-border-subtle/60',
        className,
      )}
      aria-hidden
    />
  );
}

/** Five `NodeCard`-sized skeletons matching the Dashboard's grid. */
export function NodeCardGridSkeleton({ count = 5 }: { count?: number }) {
  return (
    <div className="grid grid-cols-2 lg:grid-cols-3 gap-4 max-w-5xl mx-auto">
      {Array.from({ length: count }).map((_, i) => (
        <Skeleton key={i} className="h-[210px]" />
      ))}
    </div>
  );
}

/** A multi-row skeleton for log table-style views. */
export function TableSkeleton({ rows = 8 }: { rows?: number }) {
  return (
    <div className="space-y-1">
      {Array.from({ length: rows }).map((_, i) => (
        <Skeleton key={i} className="h-7 w-full" />
      ))}
    </div>
  );
}
