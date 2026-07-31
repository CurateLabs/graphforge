#!/usr/bin/env node
/** Refresh the checked-in GraphForge VS Code documentation snapshot via authenticated gh. */
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const scriptRoot = path.dirname(fileURLToPath(import.meta.url));
const siteRoot = path.resolve(scriptRoot, '..');
const manifestPath = path.join(siteRoot, 'external-docs.json');
const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
const revision = process.argv[2];

if (!revision || !/^[0-9a-f]{40}$/.test(revision)) {
  throw new Error('Usage: pnpm docs:update-extension <full-40-character-commit-sha>');
}

function api(endpoint, jq) {
  return execFileSync('gh', ['api', endpoint, '--jq', jq], {
    encoding: 'utf8',
  }).trim();
}

const directoryEndpoint = `repos/${manifest.repository}/contents/${manifest.sourceDirectory}?ref=${revision}`;
const actualFiles = JSON.parse(api(directoryEndpoint, '[.[].name]')).sort();
const expectedFiles = ['README.md', ...manifest.pages.map((page) => page.source)].sort();
if (JSON.stringify(actualFiles) !== JSON.stringify(expectedFiles)) {
  throw new Error(
    `Published extension docs changed. Review and explicitly map the new contract before updating.\nExpected: ${expectedFiles.join(', ')}\nActual: ${actualFiles.join(', ')}`,
  );
}

const snapshotRoot = path.join(siteRoot, 'external', 'graphforge-vscode');
fs.mkdirSync(snapshotRoot, { recursive: true });
for (const page of manifest.pages) {
  const endpoint = `repos/${manifest.repository}/contents/${manifest.sourceDirectory}/${page.source}?ref=${revision}`;
  const encoded = api(endpoint, '.content').replace(/\n/g, '');
  const content = Buffer.from(encoded, 'base64');
  page.sha256 = crypto.createHash('sha256').update(content).digest('hex');
  fs.writeFileSync(path.join(snapshotRoot, page.source), content);
}
manifest.revision = revision;
fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
console.log(`Updated ${manifest.pages.length} extension pages from ${manifest.repository}@${revision}`);
