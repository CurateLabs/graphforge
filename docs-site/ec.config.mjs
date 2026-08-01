// @ts-check
import { defineEcConfig } from '@astrojs/starlight/expressive-code';

/**
 * Expressive Code ships a deferred ResizeObserver script that sets tabindex on
 * overflowing <pre> blocks. Lighthouse/axe often audit before that runs, so
 * emit tabindex statically for keyboard access to horizontally scrollable code.
 *
 * @param {import('hast').Element | undefined} node
 * @returns {import('hast').Element | null}
 */
function findPre(node) {
  if (!node || node.type !== 'element') return null;
  if (node.tagName === 'pre') return node;
  for (const child of node.children ?? []) {
    const found = findPre(/** @type {import('hast').Element} */ (child));
    if (found) return found;
  }
  return null;
}

function staticScrollableTabindex() {
  return {
    name: 'static-scrollable-tabindex',
    hooks: {
      postprocessRenderedBlock: ({ renderData }) => {
        const pre = findPre(renderData.blockAst);
        if (!pre) return;
        const props = pre.properties ?? {};
        if (props.tabindex != null || props.tabIndex != null) return;
        pre.properties = { ...props, tabindex: 0 };
      },
    },
  };
}

/**
 * Night Owl Light comments (#989fb1 / #939dbb) stay washed out even after EC's
 * default 5.5:1 pass. Prefer stronger slate comments, then let
 * minSyntaxHighlightingColorContrast finish keywords/strings.
 *
 * @param {string | string[] | undefined} scope
 */
function isCommentScope(scope) {
  const scopes = Array.isArray(scope) ? scope : scope ? [scope] : [];
  return scopes.some(
    (s) =>
      typeof s === 'string' &&
      (s === 'comment' || s.startsWith('comment.') || s.includes('.comment')),
  );
}

export default defineEcConfig({
  plugins: [staticScrollableTabindex()],
  // Soft Night Owl tokens need more than the default 5.5:1 on light code bg.
  minSyntaxHighlightingColorContrast: 7,
  styleOverrides: {
    // Roboto Mono at 400 reads thin on light gray; 500 matches marketing density.
    codeFontWeight: '500',
  },
  customizeTheme: (theme) => {
    // Light ≈7:1 on Starlight contrast-check bg (#f6f7f9). Dark: slate muted.
    const commentFg = theme.type === 'light' ? '#4b5563' : '#94a3b8';
    for (const setting of theme.settings) {
      if (!isCommentScope(setting.scope)) continue;
      if (!setting.settings) setting.settings = {};
      setting.settings.foreground = commentFg;
    }
  },
});
