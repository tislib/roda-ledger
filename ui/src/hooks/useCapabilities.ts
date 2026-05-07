import type { Capability } from '@/types/cluster';
import { useServerInfo } from '@/hooks/useClusterSnapshot';

/**
 * Returns true iff the connected server has advertised the given capability.
 * While the GetServerInfo call is in flight (or has errored), every
 * capability check returns false — UI hides advanced controls by default
 * and only shows them once the server has confirmed support. That avoids
 * the user clicking a Kill button against a backend that returns
 * UNIMPLEMENTED.
 */
export function useHasCapability(cap: Capability): boolean {
  const { data } = useServerInfo();
  return data?.capabilities.includes(cap) ?? false;
}
