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
 * contrast pass. Prefer stronger slate comments.
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

/** Light-theme code surface: slightly off-white, near-black ink. */
const LIGHT_CODE_BG = '#f3f4f6';
const LIGHT_CODE_FG = '#111827';
const LIGHT_COMMENT_FG = '#374151';

export default defineEcConfig({
  plugins: [staticScrollableTabindex()],
  // Soft Night Owl tokens need far more than the default 5.5:1 on light code bg.
  // 10:1 pulls shell/keyword blues (e.g. #325193) down toward slate/near-black.
  minSyntaxHighlightingColorContrast: 10,
  styleOverrides: {
    // Roboto Mono at 400 reads thin on light gray; 500 matches marketing density.
    codeFontWeight: '500',
    // [dark, light] — force readable light defaults; leave dark to the theme.
    codeForeground: ({ theme }) =>
      theme.type === 'light' ? LIGHT_CODE_FG : theme.colors['editor.foreground'],
    codeBackground: ({ theme }) =>
      theme.type === 'light' ? LIGHT_CODE_BG : theme.colors['editor.background'],
    frames: {
      // Terminal frames otherwise follow Starlight gray-7 / theme terminal.bg.
      terminalBackground: ({ theme, resolveSetting }) =>
        theme.type === 'light' ? LIGHT_CODE_BG : resolveSetting('codeBackground'),
      editorBackground: ({ theme, resolveSetting }) =>
        theme.type === 'light' ? LIGHT_CODE_BG : resolveSetting('codeBackground'),
      terminalTitlebarForeground: ({ theme }) =>
        theme.type === 'light'
          ? LIGHT_CODE_FG
          : theme.colors['titleBar.activeForeground'],
    },
  },
  customizeTheme: (theme) => {
    if (theme.type === 'light') {
      theme.bg = LIGHT_CODE_BG;
      theme.fg = LIGHT_CODE_FG;
      theme.colors['editor.background'] = LIGHT_CODE_BG;
      theme.colors['editor.foreground'] = LIGHT_CODE_FG;
      if ('terminal.background' in theme.colors) {
        theme.colors['terminal.background'] = LIGHT_CODE_BG;
      }
      for (const setting of theme.settings) {
        if (!setting.settings) setting.settings = {};
        if (isCommentScope(setting.scope)) {
          setting.settings.foreground = LIGHT_COMMENT_FG;
          continue;
        }
        // Untokenized / default-ish scopes: force near-black so shell lines
        // (npm/pnpm install fences) never paint as washed Night Owl blue-grey.
        if (!setting.settings.foreground) {
          setting.settings.foreground = LIGHT_CODE_FG;
        }
      }
      return;
    }

    // Dark: slate muted comments only.
    const commentFg = '#94a3b8';
    for (const setting of theme.settings) {
      if (!isCommentScope(setting.scope)) continue;
      if (!setting.settings) setting.settings = {};
      setting.settings.foreground = commentFg;
    }
  },
});
