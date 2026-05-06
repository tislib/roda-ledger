import { createHighlighterCore, type HighlighterCore } from 'shiki/core';
import { createOnigurumaEngine } from 'shiki/engine/oniguruma';

let highlighter: Promise<HighlighterCore> | null = null;

export function getRustHighlighter(): Promise<HighlighterCore> {
  if (!highlighter) {
    highlighter = createHighlighterCore({
      themes: [import('shiki/themes/github-dark-default.mjs')],
      langs: [import('shiki/langs/rust.mjs')],
      engine: createOnigurumaEngine(import('shiki/wasm')),
    });
  }
  return highlighter;
}

export async function highlightRust(source: string): Promise<string> {
  const h = await getRustHighlighter();
  return h.codeToHtml(source, { lang: 'rust', theme: 'github-dark-default' });
}
