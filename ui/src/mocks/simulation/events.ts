import type { ElectionEvent, Role } from '@/types/cluster';

export type SimEvent =
  | { type: 'roleChanged'; nodeId: string; from: Role; to: Role; term: string }
  | { type: 'termBumped'; nodeId: string; term: string }
  | { type: 'commitAdvanced'; nodeId: string; commitIndex: string }
  | { type: 'clusterCommitAdvanced'; commitIndex: string }
  | { type: 'election'; event: ElectionEvent }
  | { type: 'healthChanged'; nodeId: string };

type Listener = (e: SimEvent) => void;

export class EventBus {
  private listeners = new Set<Listener>();

  emit(e: SimEvent): void {
    for (const listener of this.listeners) {
      try {
        listener(e);
      } catch (err) {
        console.error('SimEvent listener threw:', err);
      }
    }
  }

  subscribe(cb: Listener): () => void {
    this.listeners.add(cb);
    return () => {
      this.listeners.delete(cb);
    };
  }
}
