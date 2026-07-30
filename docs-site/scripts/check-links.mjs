#!/usr/bin/env node
/**
 * Check internal hrefs in the built Starlight dist/ against published files.
 * Run after `pnpm build` (or via `pnpm check-links`).
 *
 * Exit 1 when any same-site / root-relative link is missing from dist, or when
 * DecisionNerd / stale `/graphforge` leftovers remain in the built HTML.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const siteRoot = path.resolve(__dirname, '..');
const dist = path.join(siteRoot, 'dist');
const SITE_ORIGIN = 'https://docs.graphforge.sh';
const SITE_BASE = '';

if (!fs.existsSync(dist)) {
  console.error('Missing docs-site/dist — run `pnpm build` first.');
  process.exit(2);
}

const htmlFiles = [];
function walk(dir) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) walk(full);
    else if (entry.name.endsWith('.html')) htmlFiles.push(full);
  }
}
walk(dist);

const published = new Set();
function addPublished(urlPath) {
  published.add(urlPath);
  if (urlPath.endsWith('/')) published.add(urlPath.slice(0, -1));
  else published.add(`${urlPath}/`);
}

for (const file of htmlFiles) {
  const rel = '/' + path.relative(dist, file).replace(/\\/g, '/');
  if (rel.endsWith('/index.html')) {
    const dir = rel.slice(0, -'/index.html'.length);
    addPublished(SITE_BASE + (dir || '') + '/');
  } else if (rel.endsWith('.html')) {
    addPublished(SITE_BASE + rel.slice(0, -5));
  }
}

function walkAll(dir) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    const url = SITE_BASE + '/' + path.relative(dist, full).replace(/\\/g, '/');
    if (entry.isDirectory()) walkAll(full);
    else published.add(url);
  }
}
walkAll(dist);

const hrefRe = /href=["']([^"']+)["']/gi;
const srcRe = /(?:src)=["']([^"']+)["']/gi;
const broken = [];
const stale = [];
let checked = 0;

function pageUrlFor(file) {
  let rel = '/' + path.relative(dist, file).replace(/\\/g, '/');
  if (rel.endsWith('/index.html')) rel = rel.slice(0, -'index.html'.length);
  else if (rel.endsWith('.html')) rel = rel.slice(0, -5) + '/';
  return SITE_BASE + rel;
}

function consider(pageUrl, href, { assetsOnly = false } = {}) {
  if (!href || href.startsWith('data:') || href.startsWith('mailto:') || href.startsWith('javascript:')) {
    return;
  }
  if (href.startsWith('#')) return;
  // Ignore non-URL src values (e.g. Python kwargs rendered in headings).
  if (assetsOnly && !/^(?:https?:|\/|\.\/|\.\.\/)/i.test(href)) return;

  if (
    /decisionnerd\.github\.io|github\.com\/DecisionNerd|codecov\.io\/gh\/DecisionNerd|app\.codecov\.io\/gh\/DecisionNerd/i.test(
      href,
    )
  ) {
    stale.push({ page: pageUrl, href, reason: 'DecisionNerd leftover' });
  }
  if (
    href === '/graphforge' ||
    href.startsWith('/graphforge/') ||
    href === '/graphforge-legecy' ||
    href.startsWith('/graphforge-legecy/') ||
    href.includes('://decisionnerd.github.io/graphforge') ||
    /https?:\/\/curatelabs\.github\.io\/graphforge(?:[/?#]|$)/i.test(href) ||
    href.includes('://curatelabs.github.io/graphforge-legecy')
  ) {
    stale.push({ page: pageUrl, href, reason: 'stale GitHub Pages project base' });
  }

  let pathname;
  if (/^https?:\/\//i.test(href)) {
    const url = new URL(href);
    if (url.origin === SITE_ORIGIN || url.origin === 'http://localhost:4321') {
      pathname = url.pathname;
    } else {
      return;
    }
  } else if (href.startsWith('/')) {
    pathname = href.split('#')[0].split('?')[0];
  } else {
    try {
      pathname = new URL(href, `https://example.com${pageUrl}`).pathname;
    } catch {
      return;
    }
  }

  checked += 1;
  const clean = pathname.split('#')[0].split('?')[0];
  if (!published.has(clean) && !published.has(clean.endsWith('/') ? clean.slice(0, -1) : `${clean}/`)) {
    broken.push({ page: pageUrl, href, resolved: clean, reason: 'missing in dist' });
  }
}

for (const file of htmlFiles) {
  const html = fs.readFileSync(file, 'utf8');
  const pageUrl = pageUrlFor(file);

  for (const match of html.matchAll(hrefRe)) {
    consider(pageUrl, match[1]);
  }
  for (const match of html.matchAll(srcRe)) {
    consider(pageUrl, match[1], { assetsOnly: true });
  }
}

const uniqBroken = [...new Map(broken.map((b) => [`${b.page}::${b.href}`, b])).values()];
const uniqStale = [...new Map(stale.map((s) => [`${s.page}::${s.href}`, s])).values()];

console.log(
  JSON.stringify(
    {
      pages: htmlFiles.length,
      checked,
      broken: uniqBroken.length,
      stale: uniqStale.length,
      brokenSample: uniqBroken.slice(0, 50),
      staleSample: uniqStale.slice(0, 50),
    },
    null,
    2,
  ),
);

if (uniqBroken.length || uniqStale.length) {
  console.error(
    `\nlink check failed: ${uniqBroken.length} broken, ${uniqStale.length} stale repository/site hrefs`,
  );
  process.exit(1);
}

console.error(`\nlink check ok: ${checked} hrefs across ${htmlFiles.length} pages`);
