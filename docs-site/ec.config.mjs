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

export default defineEcConfig({
  plugins: [staticScrollableTabindex()],
});
