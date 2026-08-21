#!/usr/bin/env node
/** Sync allowlisted docs into Starlight content collection. */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const siteRoot = path.resolve(__dirname, '..');
const repoRoot = path.resolve(siteRoot, '..');
const contentRoot = path.join(siteRoot, 'src/content/docs');
const docsRoot = path.join(repoRoot, 'docs');
const externalManifest = JSON.parse(fs.readFileSync(path.join(siteRoot, 'external-docs.json'), 'utf8'));

const GH_DOCS_BLOB = 'https://github.com/CurateLabs/graphforge/blob/main/docs';
const GH_REPO_BLOB = 'https://github.com/CurateLabs/graphforge/blob/main';
const GH_DOCS_TREE = 'https://github.com/CurateLabs/graphforge/tree/main/docs';

/** Directory hrefs with no published index → concrete allowlisted page (docs-relative). */
const DIRECTORY_DEFAULTS = {
  guide: 'guide/overview.md',
  'guide/vscode-extension': 'guide/vscode-extension/index.md',
  'guide/datasets': 'guide/datasets/overview.md',
  'book/architecture': 'book/architecture/overview.md',
  'book/use-cases': 'book/use-cases/knowledge-graph-construction.md',
  'book/research': 'book/research/llm-workflows.md',
  reference: 'reference/api.md',
  development: 'development/contributing.md',
  legal: 'legal/licensing.md',
};

/**
 * Public site pages (allowlist). Published left nav is reader-journey ordered in
 * `astro.config.mjs`; paths here keep the Guide / Book / Reference / engineering
 * on-disk layout so URLs stay stable. Only explicitly allowlisted public sources
 * are eligible for publication.
 */
const PAGES = [
  'index.md',
  // Public documentation map
  'README.md',
  // Public contributor engineering lifecycle (ADRs under Engineering per #2771)
  'engineering/README.md',
  'engineering/ARCHITECTURE.md',
  'engineering/TESTING.md',
  'engineering/PUBLISHING.md',
  'engineering/OBSERVABILITY.md',
  'engineering/adrs/README.md',
  // Guide — basic usage
  'guide/overview.md',
  'guide/installation.md',
  'guide/quickstart.md',
  'guide/tutorial.md',
  'guide/cypher-guide.md',
  'guide/graph-construction.md',
  'guide/portable-projects.md',
  'guide/analytics-integration.md',
  'guide/visualization.md',
  'guide/exploratory-analyst.md',
  'guide/visualization-limits.md',
  'guides/repository-integration.md',
  'guide/datasets/overview.md',
  'guide/datasets/ldbc.md',
  'guide/datasets/neo4j-examples.md',
  'guide/datasets/networkrepository.md',
  'guide/datasets/snap.md',
  'guide/datasets/cypher-script-loading.md',
  // Book — architecture, research, deeper usage
  'book/README.md',
  'book/architecture/overview.md',
  'book/architecture/graphforge-vs-neo4j-gds.md',
  'book/architecture/storage.md',
  'book/architecture/concurrency-recovery.md',
  'book/architecture/project-format-compatibility.md',
  'book/architecture/portable-project-v2.md',
  'book/architecture/composable-multi-ontology.md',
  'book/architecture/canonical-fingerprints-v1.md',
  'book/architecture/knowledge-ledger.md',
  'book/architecture/knowledge-public-api-v1.md',
  'book/architecture/ast-and-planning.md',
  'book/architecture/execution-model.md',
  'book/architecture/algorithms.md',
  // Linked from published Book/Guide/ADR pages after #2738 IA move
  'book/architecture/refactor-v0.5.md',
  'book/architecture/embedding-v1.md',
  'book/architecture/algorithm-invocation-descriptor-v1.md',
  'book/use-cases/README.md',
  'book/use-cases/knowledge-graph-construction.md',
  'book/use-cases/network-analysis.md',
  'book/use-cases/llm-workflows.md',
  'book/use-cases/agent-grounding.md',
  'book/use-cases/agent-tool-recall.md',
  'book/research/README.md',
  'book/research/kg-construction.md',
  'book/research/network-analysis.md',
  'book/research/analyst-verbs-at-scale.md',
  'book/research/llm-workflows.md',
  'book/research/llm-context-building.md',
  'book/research/agent-grounding.md',
  'book/research/search-entity-resolution.md',
  'book/research/genealogy.md',
  // Reference + contributor surfaces
  'reference/api.md',
  'reference/opencypher-compatibility.md',
  'reference/tck-compliance.md',
  'reference/scale-limits.md',
  'reference/graph-scale-index.md',
  'reference/scale-evaluation.md',
  'reference/load-matrix-results.md',
  'reference/column-naming-behavior.md',
  'development/contributing.md',
  'development/workflow.md',
  'development/testing.md',
  'development/release-load-matrix.md',
  'development/release-process.md',
  'development/publication-order.md',
  'development/clean-environment-verification.md',
  'development/release-workflows.md',
  // Accepted ADRs after #2725 cull/renumber.
  'adr/README.md',
  'adr/0001-rust-core.md',
  'adr/0002-lr1-grammar.md',
  'adr/0003-progressive-ontology.md',
  'adr/0004-adjacency-index.md',
  'adr/0005-layered-architecture.md',
  'adr/0006-epistemic-model.md',
  'adr/0007-temporal-values.md',
  'adr/0008-heterogeneous-lists.md',
  'adr/0009-nested-heterogeneous-lists.md',
  'adr/0010-wide-date-and-duration.md',
  'adr/0011-dynamic-heterogeneous-values.md',
  'adr/0012-knowledge-domain-ownership.md',
  'adr/0013-project-generation-protocol.md',
  'adr/0014-workspace-checkpoints.md',
  'adr/0015-embedded-write-modes.md',
  'adr/0016-repository-integration-and-deployment-configuration.md',
  'adr/0017-unified-release-version.md',
  'adr/0018-acknowledged-durability-isolation.md',
  'adr/0019-authoritative-graph-delta-journal.md',
  'adr/0020-ntfs-write-through-namespace-durability.md',
  'adr/0021-portable-project-v2.md',
  'adr/0023-composable-multi-ontology.md',
  'releases/roadmap.md',
  'legal/licensing.md',
  'community/security.md',
  'community/code-of-conduct.md',
];

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function rimrafContent(dir) {
  if (!fs.existsSync(dir)) return;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      fs.rmSync(full, { recursive: true, force: true });
    } else {
      fs.unlinkSync(full);
    }
  }
}

function titleFromMarkdown(md, fallback) {
  const fm = md.match(/^---\r?\n([\s\S]*?)\r?\n---\r?\n/);
  const body = fm ? md.slice(fm[0].length) : md;
  const h1 = body.match(/^#\s+(.+)$/m);
  if (h1) return h1[1].trim();
  return fallback;
}

function stripFirstH1(md) {
  // No `$` without /m — otherwise `.+` swallows the whole file.
  return md.replace(/^#\s+.+\r?\n/, '');
}

function upsertFrontmatter(md, title) {
  const fmMatch = md.match(/^---\r?\n([\s\S]*?)\r?\n---\r?\n/);
  if (fmMatch) {
    let fmBody = fmMatch[1];
    if (!/^\s*title\s*:/m.test(fmBody)) {
      fmBody = `title: ${JSON.stringify(title)}\n${fmBody}`;
    }
    const body = stripFirstH1(md.slice(fmMatch[0].length)).replace(/^\s*\n/, '');
    return `---\n${fmBody}\n---\n\n${body}`;
  }
  const body = stripFirstH1(md).replace(/^\s*\n/, '');
  return `---\ntitle: ${JSON.stringify(title)}\n---\n\n${body}`;
}

function convertAdmonitions(md) {
  // MkDocs Material:
  //   !!! type "title"
  //       indented body
  // or unindented body until the next blank line.
  return md.replace(
    /^!!!\s+(\w+)(?:\s+"([^"]*)")?\s*\r?\n([\s\S]*?)(?=\r?\n(?:!!!|\s*#|\s*---|\s*$|\r?\n))/gm,
    (_m, type, title, body) => {
      const lines = body.replace(/\r?\n$/, '').split(/\r?\n/);
      const indented = lines.every((line) => line === '' || /^\s+/.test(line));
      let cleaned;
      if (indented) {
        cleaned = lines.map((line) => line.replace(/^[ \t]{4}/, '').replace(/^[ \t]+/, '')).join('\n');
      } else {
        // Take contiguous non-empty lines after the admonition marker.
        const kept = [];
        for (const line of lines) {
          if (line.trim() === '') break;
          kept.push(line);
        }
        cleaned = kept.join('\n');
      }
      cleaned = cleaned.replace(/\n+$/, '');
      const label = title ? `[${title}]` : '';
      return `:::${type}${label}\n${cleaned}\n:::\n`;
    },
  );
}

function neutralizeMkdocstrings(md) {
  return md.replace(
    /^:::\s+graphforge[\w.]*\s*$/gm,
    '> Python recipe API details are documented in the GraphForge Python package docstrings.',
  );
}

/** Map a docs-relative source path to its Starlight slug (no leading/trailing slash). */
function sourcePathToSlug(sourceRel) {
  const normalized = sourceRel.replace(/\\/g, '/').replace(/^\.\//, '');
  if (normalized === 'index.md') return '';
  const parts = normalized.split('/');
  const out = [];
  for (let i = 0; i < parts.length; i++) {
    const part = parts[i];
    const isLast = i === parts.length - 1;
    if (!isLast) {
      out.push(part.toLowerCase());
      continue;
    }
    const ext = path.extname(part);
    const stem = path.basename(part, ext);
    if (stem.toLowerCase() === 'index') {
      return out.join('/');
    }
    if (stem.toLowerCase() === 'readme') {
      if (i === 0) return 'documentation';
      return out.join('/');
    }
    out.push(stem.toLowerCase().replace(/\./g, '-'));
  }
  return out.join('/');
}

/** Destination content-collection relative path for an allowlisted source. */
function sourcePathToDestRel(sourceRel) {
  return sourceRel
    .split('/')
    .map((part, idx, arr) => {
      if (idx === arr.length - 1) {
        const ext = path.extname(part);
        const stem = path.basename(part, ext);
        if (stem.toLowerCase() === 'readme') {
          return idx === 0 ? `documentation${ext.toLowerCase()}` : `index${ext.toLowerCase()}`;
        }
        return `${stem.toLowerCase().replace(/\./g, '-')}${ext.toLowerCase()}`;
      }
      return part.toLowerCase();
    })
    .join('/');
}

function siteHrefForSlug(fromSlug, toSlug, hash) {
  // Pages are served as SITE_BASE/<slug>/ (directory URLs). Compute a relative
  // href from the current page directory so links work under the project base.
  const fromDir = fromSlug === '' ? '.' : fromSlug;
  const toDir = toSlug === '' ? '.' : toSlug;
  let rel = path.posix.relative(fromDir, toDir);
  if (rel === '') rel = '.';
  if (!rel.startsWith('.') && !rel.startsWith('/')) rel = `./${rel}`;
  if (rel === '.') {
    return hash ? `./${hash}` : './';
  }
  if (!rel.endsWith('/')) rel += '/';
  return `${rel}${hash || ''}`;
}

function resolveDocsTarget(fromRel, hrefPath) {
  const fromDir = path.posix.dirname(fromRel.replace(/\\/g, '/'));
  let target = hrefPath.replace(/\\/g, '/');
  if (target.startsWith('/')) {
    // Treat as docs-root-relative without leading slash.
    target = target.replace(/^\//, '');
  } else {
    target = path.posix.normalize(path.posix.join(fromDir === '.' ? '' : fromDir, target));
    // `../../AGENTS.md` from docs/… escapes the docs tree into the repo root.
    if (target.startsWith('../') || target === '..') {
      const repoPath = target.replace(/^(\.\.\/)+/, '');
      if (
        repoPath &&
        !repoPath.includes('..') &&
        fs.existsSync(path.join(repoRoot, repoPath))
      ) {
        return { kind: 'github-repo', path: repoPath };
      }
      return null;
    }
  }
  // Strip trailing slash for lookup.
  const trimmed = target.replace(/\/$/, '');
  if (DIRECTORY_DEFAULTS[trimmed]) {
    return { kind: 'md', path: DIRECTORY_DEFAULTS[trimmed] };
  }
  if (trimmed === 'contracts' || target === 'contracts/') {
    return { kind: 'github-tree', path: 'contracts' };
  }
  if (/\.(json|sha256)$/i.test(trimmed)) {
    return { kind: 'github-docs', path: trimmed };
  }
  if (/\.ipynb$/i.test(trimmed)) {
    // notebooks live at repo root, not under docs/
    const repoPath = trimmed.startsWith('examples/')
      ? trimmed
      : trimmed.replace(/^(\.\.\/)+/, '');
    return { kind: 'github-repo', path: repoPath };
  }
  if (trimmed.endsWith('.md')) {
    return { kind: 'md', path: trimmed };
  }
  // Extensionless / directory: try README, overview, or .md
  const candidates = [
    `${trimmed}.md`,
    `${trimmed}/README.md`,
    `${trimmed}/overview.md`,
    `${trimmed}/index.md`,
  ];
  for (const c of candidates) {
    if (fs.existsSync(path.join(docsRoot, c))) {
      if (c.endsWith('README.md') || c.endsWith('overview.md') || c.endsWith('index.md') || c.endsWith('.md')) {
        // Prefer DIRECTORY_DEFAULTS when only a loose directory was linked and README isn't published
        return { kind: 'md', path: c };
      }
    }
  }
  if (fs.existsSync(path.join(docsRoot, trimmed)) && fs.statSync(path.join(docsRoot, trimmed)).isDirectory()) {
    if (DIRECTORY_DEFAULTS[trimmed]) return { kind: 'md', path: DIRECTORY_DEFAULTS[trimmed] };
    return { kind: 'github-tree', path: trimmed };
  }
  return null;
}

/**
 * MkDocs resolved `.md` links to pretty URLs. Starlight leaves them literal, and
 * directory-URL pages make sibling `foo.md` hrefs resolve under the wrong path.
 * Rewrite in-content links to correct site-relative hrefs (or GitHub for artifacts).
 */
function rewriteMarkdownLinks(md, fromRel) {
  const fromSlug = sourcePathToSlug(fromRel);
  const allowlist = new Set([...PAGES, ...externalManifest.pages.map((page) => page.destination)]);

  // Protect fenced code blocks from link rewriting.
  const fences = [];
  const withoutFences = md.replace(/```[\s\S]*?```/g, (block) => {
    const token = `\0FENCE${fences.length}\0`;
    fences.push(block);
    return token;
  });

  const rewriteHref = (text, href, wrap) => {
    if (/^(https?:|mailto:|tel:)/i.test(href)) return null;
    if (href.startsWith('#')) return null;

    const hashMatch = href.match(/(#[^)]*)$/);
    const hash = hashMatch ? hashMatch[1] : '';
    const hrefPath = hash ? href.slice(0, -hash.length) : href;
    if (!hrefPath) return null;

    const resolved = resolveDocsTarget(fromRel, hrefPath);
    if (!resolved) return null;

    let next;
    if (resolved.kind === 'github-docs') {
      next = `${GH_DOCS_BLOB}/${resolved.path}${hash}`;
    } else if (resolved.kind === 'github-repo') {
      next = `${GH_REPO_BLOB}/${resolved.path}${hash}`;
    } else if (resolved.kind === 'github-tree') {
      next = `${GH_DOCS_TREE}/${resolved.path}${hash}`;
    } else if (resolved.kind === 'md') {
      if (!allowlist.has(resolved.path) && !allowlist.has(resolved.path.replace(/\\/g, '/'))) {
        // Unpublished markdown: send readers to GitHub rather than a site 404.
        next = `${GH_DOCS_BLOB}/${resolved.path}${hash}`;
      } else {
        const toSlug = sourcePathToSlug(resolved.path);
        next = siteHrefForSlug(fromSlug, toSlug, hash);
      }
    } else {
      return null;
    }
    return wrap(text, next);
  };

  // Nested image badges: [![alt](img)](href) — rewrite the outer href first so the
  // simpler `[text](href)` pass does not stop at the inner `](`.
  let rewritten = withoutFences.replace(
    /\[(!\[[^\]]*\]\([^)\s]+\))\]\(([^)\s]+)\)/g,
    (full, imageMd, href) => {
      const out = rewriteHref(imageMd, href, (text, next) => `[${text}](${next})`);
      return out ?? full;
    },
  );

  rewritten = rewritten.replace(/\[([^\]]*)\]\(([^)\s]+)\)/g, (full, text, href) => {
    const out = rewriteHref(text, href, (t, next) => `[${t}](${next})`);
    return out ?? full;
  });

  return rewritten.replace(/\0FENCE(\d+)\0/g, (_m, i) => fences[Number(i)]);
}

function validateExternalManifest(manifest) {
  if (!/^[0-9a-f]{40}$/.test(manifest.revision)) {
    throw new Error('External docs revision must be an immutable full commit SHA');
  }
  if (manifest.sourceDirectory !== 'docs/published') {
    throw new Error('External docs may only be imported from docs/published');
  }
  if (!/^[\w.-]+\/[\w.-]+$/.test(manifest.repository)) {
    throw new Error(`Invalid external docs repository: ${manifest.repository}`);
  }
  const expected = new Set(['overview.md', 'install.md', 'commands.md', 'agent-interop.md']);
  const sources = new Set(manifest.pages.map((page) => page.source));
  if (
    manifest.pages.length !== expected.size ||
    sources.size !== expected.size ||
    [...expected].some((source) => !sources.has(source))
  ) {
    throw new Error(`External docs allowlist must be exactly: ${[...expected].join(', ')}`);
  }
  const destinations = new Set(manifest.pages.map((page) => page.destination));
  if (destinations.size !== manifest.pages.length) {
    throw new Error('External docs destinations must be unique');
  }
  for (const page of manifest.pages) {
    if (page.source.includes('/') || page.source.includes('..')) {
      throw new Error(`External docs source escapes docs/published: ${page.source}`);
    }
    if (!/^guide\/vscode-extension\/[a-z0-9-]+\.md$/.test(page.destination)) {
      throw new Error(`Invalid external docs destination: ${page.destination}`);
    }
    if (!/^[0-9a-f]{64}$/.test(page.sha256)) {
      throw new Error(`Invalid SHA-256 for external docs page: ${page.source}`);
    }
  }
}

function rewriteExternalMarkdownLinks(md, page, manifest) {
  const mapping = new Map(manifest.pages.map((item) => [item.source, item.destination]));
  const fromSlug = sourcePathToSlug(page.destination);
  const sourceBase = `https://github.com/${manifest.repository}/blob/${manifest.revision}/${manifest.sourceDirectory}`;
  const fences = [];
  const withoutFences = md.replace(/```[\s\S]*?```/g, (block) => {
    const token = `\0EXTERNAL_FENCE${fences.length}\0`;
    fences.push(block);
    return token;
  });

  const rewriteHref = (text, href, wrap) => {
    if (/^(https?:|mailto:|tel:|#)/i.test(href)) return null;
    const [hrefPath, hashPart] = href.split('#', 2);
    const normalized = path.posix.normalize(path.posix.join(path.posix.dirname(page.source), hrefPath));
    const hash = hashPart ? `#${hashPart}` : '';
    const destination = mapping.get(normalized);
    if (destination) {
      return wrap(text, siteHrefForSlug(fromSlug, sourcePathToSlug(destination), hash));
    }
    return wrap(text, `${sourceBase}/${normalized}${hash}`);
  };

  let rewritten = withoutFences.replace(
    /\[(!\[[^\]]*\]\([^)\s]+\))\]\(([^)\s]+)\)/g,
    (full, imageMd, href) => {
      const out = rewriteHref(imageMd, href, (text, next) => `[${text}](${next})`);
      return out ?? full;
    },
  );

  rewritten = rewritten.replace(/\[([^\]]*)\]\(([^)\s]+)\)/g, (full, text, href) => {
    const out = rewriteHref(text, href, (label, next) => `[${label}](${next})`);
    return out ?? full;
  });
  return rewritten.replace(/\0EXTERNAL_FENCE(\d+)\0/g, (_match, index) => fences[Number(index)]);
}

function addExternalSourceMetadata(md, page, manifest) {
  const sourceUrl = `https://github.com/${manifest.repository}/blob/${manifest.revision}/${manifest.sourceDirectory}/${page.source}`;
  md = md.replace(/^---\n/, `---\neditUrl: ${JSON.stringify(sourceUrl)}\n`);
  return md.replace(
    /\n---\n\n/,
    `\n---\n\n> This page is synchronized from [\`${manifest.repository}\`](${sourceUrl}) at revision [\`${manifest.revision.slice(0, 12)}\`](https://github.com/${manifest.repository}/commit/${manifest.revision}).\n\n`,
  );
}

async function syncExternalDocs(manifest) {
  validateExternalManifest(manifest);
  let imported = 0;
  for (const page of manifest.pages) {
    const snapshot = path.join(siteRoot, 'external', 'graphforge-vscode', page.source);
    if (!fs.existsSync(snapshot)) {
      throw new Error(`Missing external docs snapshot: ${snapshot}`);
    }
    let md = fs.readFileSync(snapshot, 'utf8');
    const actualSha = crypto.createHash('sha256').update(md).digest('hex');
    if (actualSha !== page.sha256) {
      throw new Error(`External docs checksum mismatch for ${page.source}: expected ${page.sha256}, got ${actualSha}`);
    }
    const title = titleFromMarkdown(md, path.basename(page.source, '.md'));
    md = upsertFrontmatter(md, title);
    md = rewriteExternalMarkdownLinks(md, page, manifest);
    md = addExternalSourceMetadata(md, page, manifest);
    const destination = path.join(contentRoot, page.destination);
    ensureDir(path.dirname(destination));
    fs.writeFileSync(destination, md);
    imported += 1;
  }
  console.log(
    `Imported ${imported} extension pages from the verified ${manifest.repository}@${manifest.revision} snapshot`,
  );
  return imported;
}

rimrafContent(contentRoot);
ensureDir(contentRoot);

let count = 0;
for (const rel of PAGES) {
  const src = path.join(docsRoot, rel);
  if (!fs.existsSync(src)) {
    throw new Error(`Missing allowlisted documentation source: ${rel}`);
  }
  let md = fs.readFileSync(src, 'utf8');
  const fallbackTitle = path.basename(rel, path.extname(rel));
  const title = titleFromMarkdown(md, fallbackTitle);
  md = upsertFrontmatter(md, title);
  md = convertAdmonitions(md);
  md = neutralizeMkdocstrings(md);
  md = rewriteMarkdownLinks(md, rel);

  const destRel = sourcePathToDestRel(rel);
  const dest = path.join(contentRoot, destRel);
  ensureDir(path.dirname(dest));
  fs.writeFileSync(dest, md);
  count += 1;
}

count += await syncExternalDocs(externalManifest);

console.log(`Synced ${count} pages into Starlight content collection`);
