import type { HTMLAttributes } from 'react';
import { cn } from '@/lib/cn';

type Props = HTMLAttributes<HTMLSpanElement>;

export function MonoText({ className, ...rest }: Props) {
  return <span className={cn('font-mono', className)} {...rest} />;
}
