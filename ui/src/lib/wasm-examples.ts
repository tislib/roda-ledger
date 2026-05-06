/**
 * Pre-built WASM extension examples shown on the Meta page.
 *
 * These are local UI fixtures — not part of the cluster client contract.
 * The real backend has no notion of "examples"; this module exists so the
 * UI can offer a one-click deploy library without server cooperation.
 */
import type { WasmFunction } from '@/types/wasm';
import { WASM_EXAMPLES as MOCK_EXAMPLES } from '@/mocks/fixtures/wasm-examples';

export const EXAMPLE_FUNCTIONS: readonly WasmFunction[] = MOCK_EXAMPLES;

export function useExampleFunctions(): readonly WasmFunction[] {
  return EXAMPLE_FUNCTIONS;
}
