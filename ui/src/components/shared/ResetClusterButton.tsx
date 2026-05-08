import { useState } from 'react';
import { useResetCluster } from '@/hooks/useProvisioning';
import { dialog } from '@/lib/dialog';
import { toast } from '@/lib/toast';
import { cn } from '@/lib/cn';

interface Props {
  /** Tailwind classes for the button. Defaults to a destructive btn. */
  className?: string;
  /** Override the button label. */
  label?: string;
}

/**
 * Destructive control: wipes all cluster data and reinitialises with the
 * same config + node count. Always gated by a confirmation dialog so a
 * stray click can't blow away an active demo.
 */
export function ResetClusterButton({ className, label = 'Reset cluster' }: Props) {
  const reset = useResetCluster();
  const [pending, setPending] = useState(false);

  const onClick = async () => {
    const ok = await dialog.confirm({
      title: 'Reset cluster?',
      description:
        'This stops every node, wipes their on-disk data (WALs, snapshots, balances), clears scenario history and fault history, then reprovisions a fresh cluster with the same config and node count. The action cannot be undone.',
      confirmLabel: 'Yes, reset',
      cancelLabel: 'Cancel',
      destructive: true,
    });
    if (!ok) return;
    setPending(true);
    try {
      const result = await reset.mutateAsync();
      if (result.accepted) {
        toast.success('Cluster reset', 'Fresh cluster is up.');
      } else {
        toast.error('Reset failed', result.error || 'Unknown error');
      }
    } catch (err) {
      toast.error('Reset failed', String(err));
    } finally {
      setPending(false);
    }
  };

  return (
    <button
      onClick={onClick}
      disabled={pending || reset.isPending}
      className={cn('btn btn-danger', className)}
      title="Destroy all data and reinitialize the cluster"
    >
      {pending || reset.isPending ? 'Resetting…' : `⚠ ${label}`}
    </button>
  );
}
