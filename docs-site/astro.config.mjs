// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

/** @type {import('astro').AstroUserConfig} */
export default defineConfig({
  site: 'https://docs.graphforge.sh',
  outDir: 'dist',
  integrations: [
    starlight({
      title: 'GraphForge',
      description: 'Composable graph tooling for analysis, construction, and refinement',
      favicon: '/favicon.svg',
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/CurateLabs/graphforge',
        },
      ],
      editLink: {
        baseUrl: 'https://github.com/CurateLabs/graphforge/edit/main/docs/',
      },
      customCss: ['./src/styles/custom.css'],
      // Published nav is reader-journey ordered (Diátaxis). On-disk trees remain
      // Guide / Book / Reference; slugs are unchanged so prior URLs stay stable.
      sidebar: [
        {
          label: 'Get started',
          items: [
            { label: 'Installation', slug: 'guide/installation' },
            { label: 'Quick Start', slug: 'guide/quickstart' },
            { label: 'Tutorial', slug: 'guide/tutorial' },
          ],
        },
        {
          label: 'Use every day',
          items: [
            { label: 'Overview', slug: 'guide/overview' },
            {
              label: 'VS Code extension',
              collapsed: false,
              items: [
                { label: 'Overview', slug: 'guide/vscode-extension' },
                { label: 'Install and choose a runtime', slug: 'guide/vscode-extension/install' },
                { label: 'Commands', slug: 'guide/vscode-extension/commands' },
                { label: 'Agent interop', slug: 'guide/vscode-extension/agent-interop' },
              ],
            },
            { label: 'Cypher Query Language', slug: 'guide/cypher-guide' },
            { label: 'Graph Construction', slug: 'guide/graph-construction' },
            { label: 'Analytics Integration', slug: 'guide/analytics-integration' },
            { label: 'Exploratory Analyst', slug: 'guide/exploratory-analyst' },
          ],
        },
        {
          label: 'Understand',
          items: [
            { label: 'Overview', slug: 'book' },
            {
              label: 'Architecture',
              collapsed: true,
              items: [
                { label: 'Overview', slug: 'book/architecture/overview' },
                {
                  label: 'GraphForge and Neo4j GDS',
                  slug: 'book/architecture/graphforge-vs-neo4j-gds',
                },
                { label: 'Storage', slug: 'book/architecture/storage' },
                {
                  label: 'Pre-v1 Project Compatibility',
                  slug: 'book/architecture/project-format-compatibility',
                },
                {
                  label: 'Canonical Fingerprints v1',
                  slug: 'book/architecture/canonical-fingerprints-v1',
                },
                {
                  label: 'M20 Immutable Knowledge Ledger',
                  slug: 'book/architecture/knowledge-ledger',
                },
                { label: 'M20 Public API v1', slug: 'book/architecture/m20-public-api-v1' },
                { label: 'AST & Planning', slug: 'book/architecture/ast-and-planning' },
                { label: 'Execution Model', slug: 'book/architecture/execution-model' },
                { label: 'Algorithms', slug: 'book/architecture/algorithms' },
                { label: 'Architecture Refactor v0.5', slug: 'book/architecture/refactor-v0-5' },
                { label: 'Embedding v1', slug: 'book/architecture/embedding-v1' },
                {
                  label: 'M18 Invocation Descriptor v1',
                  slug: 'book/architecture/m18-invocation-descriptor-v1',
                },
              ],
            },
            {
              label: 'Use Cases',
              collapsed: true,
              items: [
                { label: 'Overview', slug: 'book/use-cases' },
                {
                  label: 'Knowledge Graph Construction',
                  slug: 'book/use-cases/knowledge-graph-construction',
                },
                { label: 'Network Analysis', slug: 'book/use-cases/network-analysis' },
                { label: 'LLM-Powered Workflows', slug: 'book/use-cases/llm-workflows' },
                { label: 'AI Agent Grounding', slug: 'book/use-cases/agent-grounding' },
                { label: 'AI Agent Tool Recall', slug: 'book/use-cases/agent-tool-recall' },
              ],
            },
            {
              label: 'Research',
              collapsed: true,
              items: [
                { label: 'Overview', slug: 'book/research' },
                { label: 'Knowledge Graph Construction', slug: 'book/research/kg-construction' },
                { label: 'Network Analysis', slug: 'book/research/network-analysis' },
                { label: 'Analyst Verbs at Scale', slug: 'book/research/analyst-verbs-at-scale' },
                { label: 'LLM-Powered Workflows', slug: 'book/research/llm-workflows' },
                { label: 'LLM Context Building', slug: 'book/research/llm-context-building' },
                { label: 'AI Agent Grounding', slug: 'book/research/agent-grounding' },
                {
                  label: 'Search & Entity Resolution',
                  slug: 'book/research/search-entity-resolution',
                },
                { label: 'Genealogy', slug: 'book/research/genealogy' },
              ],
            },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'API Documentation', slug: 'reference/api' },
            {
              label: 'OpenCypher Compatibility',
              slug: 'reference/opencypher-compatibility',
            },
            { label: 'TCK Compliance', slug: 'reference/tck-compliance' },
            { label: 'Scale Limits', slug: 'reference/scale-limits' },
            {
              label: 'Load Matrix Results',
              slug: 'reference/load-matrix-results',
            },
            { label: 'Column Naming', slug: 'reference/column-naming-behavior' },
            {
              // Catalog loaders are backlog — not a v0.5.0 core/product surface.
              label: 'Datasets (backlog)',
              collapsed: true,
              items: [
                { label: 'Overview', slug: 'guide/datasets/overview' },
                { label: 'LDBC', slug: 'guide/datasets/ldbc' },
                { label: 'Neo4j Examples', slug: 'guide/datasets/neo4j-examples' },
                { label: 'NetworkRepository', slug: 'guide/datasets/networkrepository' },
                { label: 'SNAP', slug: 'guide/datasets/snap' },
                { label: 'Cypher Script Loading', slug: 'guide/datasets/cypher-script-loading' },
              ],
            },
            { label: 'Changelog', slug: 'reference/changelog' },
          ],
        },
        {
          label: 'Contribute & operate',
          collapsed: true,
          items: [
            { label: 'Documentation map', slug: 'documentation' },
            { label: 'Contributing', slug: 'development/contributing' },
            { label: 'Workflow', slug: 'development/workflow' },
            { label: 'Testing Strategy', slug: 'development/testing' },
            { label: 'Release Load Matrix', slug: 'development/release-load-matrix' },
            { label: 'Release Process', slug: 'development/release-process' },
            { label: 'Publication Order', slug: 'development/publication-order' },
            {
              label: 'Clean-environment verification',
              slug: 'development/clean-environment-verification',
            },
            { label: 'Release Strategy', slug: 'development/release-strategy' },
            { label: 'Release Workflows', slug: 'development/release-workflows' },
            { label: 'Product roadmap', slug: 'releases/roadmap' },
          ],
        },
        {
          label: 'Engineering',
          collapsed: true,
          items: [
            { label: 'Overview', slug: 'engineering' },
            { label: 'Architecture', slug: 'engineering/architecture' },
            { label: 'Testing', slug: 'engineering/testing' },
            { label: 'Publishing', slug: 'engineering/publishing' },
            { label: 'Observability', slug: 'engineering/observability' },
            {
              label: 'Architecture Decision Records',
              collapsed: true,
              items: [
                { label: 'Decision log', slug: 'engineering/adrs' },
                { label: 'Index', slug: 'adr' },
                { label: '0001 — Rust Core', slug: 'adr/0001-rust-core' },
                { label: '0002 — RD+Pratt Parser', slug: 'adr/0002-lr1-grammar' },
                { label: '0003 — Progressive Ontology', slug: 'adr/0003-progressive-ontology' },
                { label: '0004 — Adjacency Index', slug: 'adr/0004-adjacency-index' },
                { label: '0005 — Layered Architecture', slug: 'adr/0005-layered-architecture' },
                { label: '0006 — Epistemic Model', slug: 'adr/0006-epistemic-model' },
                { label: '0007 — Temporal Values', slug: 'adr/0007-temporal-values' },
                { label: '0008 — Heterogeneous Lists', slug: 'adr/0008-heterogeneous-lists' },
                {
                  label: '0009 — Nested Heterogeneous Lists',
                  slug: 'adr/0009-nested-heterogeneous-lists',
                },
                {
                  label: '0010 — Wide Date and Duration',
                  slug: 'adr/0010-wide-date-and-duration',
                },
                {
                  label: '0011 — Dynamic Heterogeneous Values',
                  slug: 'adr/0011-dynamic-heterogeneous-values',
                },
                {
                  label: '0012 — M20/M21 Domain Ownership',
                  slug: 'adr/0012-m20-domain-ownership',
                },
                {
                  label: '0013 — Project Generations',
                  slug: 'adr/0013-project-generation-protocol',
                },
                {
                  label: '0014 — Workspace Checkpoints',
                  slug: 'adr/0014-workspace-checkpoints',
                },
                {
                  label: '0015 — Embedded Write Modes',
                  slug: 'adr/0015-embedded-write-modes',
                },
                {
                  label: '0016 — Repository Integration',
                  slug: 'adr/0016-repository-integration-and-deployment-configuration',
                },
              ],
            },
          ],
        },
        {
          label: 'Community',
          collapsed: true,
          items: [
            { label: 'Licensing', slug: 'legal/licensing' },
            { label: 'Security', slug: 'community/security' },
            { label: 'Code of Conduct', slug: 'community/code-of-conduct' },
          ],
        },
      ],
    }),
  ],
});
