#!/usr/bin/env node
/** Deterministic contract checks for the checked-in VS Code extension docs snapshot. */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const manifest = JSON.parse(fs.readFileSync(path.join(siteRoot, 'external-docs.json'), 'utf8'));
const expectedSources = ['agent-interop.md', 'commands.md', 'install.md', 'overview.md'];
const forbiddenPrivateMarkers = [
  'Observed need',
  'Given/When/Then',
  'Open questions',
  'docs/REQUIREMENTS',
  'docs/engineering',
  'docs/experience',
  'docs/strategy',
];

assert.match(manifest.revision, /^[0-9a-f]{40}$/, 'revision must be a full commit SHA');
assert.equal(manifest.sourceDirectory, 'docs/published');
assert.deepEqual(manifest.localPatches, [
  {
    issue: '#279',
    reason: 'Use the Curate Labs-owned npm scope for the v0.5.0 release.',
    sources: ['commands.md', 'install.md', 'overview.md'],
  },
]);
assert.match(manifest.repository, /^[\w.-]+\/[\w.-]+$/, 'repository must be a GitHub owner/name pair');
assert.equal(manifest.pages.length, expectedSources.length, 'duplicate page entries are forbidden');
assert.deepEqual(
  manifest.pages.map((page) => page.source).sort(),
  expectedSources,
  'only the approved public pages may be imported',
);
assert.equal(
  new Set(manifest.pages.map((page) => page.destination)).size,
  manifest.pages.length,
  'destinations must be unique',
);

for (const page of manifest.pages) {
  assert.match(page.destination, /^guide\/vscode-extension\/[a-z0-9-]+\.md$/);
  const snapshot = path.join(siteRoot, 'external', 'graphforge-vscode', page.source);
  const content = fs.readFileSync(snapshot);
  assert.equal(crypto.createHash('sha256').update(content).digest('hex'), page.sha256);
  const markdown = content.toString('utf8');
  for (const marker of forbiddenPrivateMarkers) {
    assert.equal(markdown.includes(marker), false, `${page.source} contains private marker: ${marker}`);
  }
}

console.log(`Extension docs contract ok: ${manifest.pages.length} pages at ${manifest.revision}`);
