import type { ReactNode } from 'react';

interface Props {
  title: string;
  hint?: ReactNode;
}

export function EmptyState({ title, hint }: Props) {
  return (
    <div className="flex flex-col items-center justify-center py-12 text-center">
      <div className="text-text-secondary text-sm">{title}</div>
      {hint && <div className="text-text-muted text-xs mt-1">{hint}</div>}
    </div>
  );
}
