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
      // Full EC options (light fg/bg, frames, customizeTheme) live in ec.config.mjs.
      expressiveCode: {
        minSyntaxHighlightingColorContrast: 10,
        styleOverrides: {
          codeFontWeight: '500',
        },
      },

      // Published nav is reader-journey ordered (Diátaxis). On-disk trees remain
      // Guide / Book / Reference; slugs are unchanged so prior URLs stay stable.
      sidebar: [
        {
          label: 'Get started',
          items: [
            { label: 'Installation', slug: 'guide/installation' },
            { label: 'Quick Start', slug: 'guide/quickstart' },
            { label: 'Tutorial', slug: 'guide/tutorial' },
            { label: 'CLI & repositories', slug: 'guides/repository-integration' },
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
            { label: 'Move portable projects', slug: 'guide/portable-projects' },
            { label: 'Analytics Integration', slug: 'guide/analytics-integration' },
            { label: 'Visualization examples', slug: 'guide/visualization' },
            { label: 'Exploratory Analyst', slug: 'guide/exploratory-analyst' },
            { label: 'Visualization limits', slug: 'guide/visualization-limits' },
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
                  label: 'Concurrency and recovery',
                  slug: 'book/architecture/concurrency-recovery',
                },
                {
                  label: 'Pre-v1 Project Compatibility',
                  slug: 'book/architecture/project-format-compatibility',
                },
                {
                  label: 'Portable Project v2',
                  slug: 'book/architecture/portable-project-v2',
                },
                {
                  label: 'Composable Multi-Ontology',
                  slug: 'book/architecture/composable-multi-ontology',
                },
                {
                  label: 'Canonical Fingerprints v1',
                  slug: 'book/architecture/canonical-fingerprints-v1',
                },
                {
                  label: 'Immutable Knowledge Ledger',
                  slug: 'book/architecture/knowledge-ledger',
                },
                {
                  label: 'Knowledge Public API',
                  slug: 'book/architecture/knowledge-public-api-v1',
                },
                { label: 'AST & Planning', slug: 'book/architecture/ast-and-planning' },
                { label: 'Execution Model', slug: 'book/architecture/execution-model' },
                { label: 'Algorithms', slug: 'book/architecture/algorithms' },
                { label: 'Architecture Refactor v0.5', slug: 'book/architecture/refactor-v0-5' },
                { label: 'Embedding v1', slug: 'book/architecture/embedding-v1' },
                {
                  label: 'Analyst Invocation Descriptor',
                  slug: 'book/architecture/algorithm-invocation-descriptor-v1',
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
              label: 'Graph Scale Index (GSI)',
              slug: 'reference/graph-scale-index',
            },
            {
              label: 'Scale Evaluation',
              slug: 'reference/scale-evaluation',
            },
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
                { label: 'LDBC full suite', slug: 'guide/datasets/ldbc' },
                { label: 'Neo4j Examples', slug: 'guide/datasets/neo4j-examples' },
                { label: 'NetworkRepository', slug: 'guide/datasets/networkrepository' },
                { label: 'SNAP', slug: 'guide/datasets/snap' },
                { label: 'Cypher Script Loading', slug: 'guide/datasets/cypher-script-loading' },
              ],
            },
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
            { label: 'Billion-edge certification', slug: 'development/g500-certification' },
            { label: 'Product roadmap', slug: 'releases/roadmap' },
            { label: 'Publishing', slug: 'engineering/publishing' },
            { label: 'Release Process', slug: 'development/release-process' },
            { label: 'Publication Order', slug: 'development/publication-order' },
            { label: 'Release Workflows', slug: 'development/release-workflows' },
            { label: 'Release Load Matrix', slug: 'development/release-load-matrix' },
            {
              label: 'Clean-environment verification',
              slug: 'development/clean-environment-verification',
            },
          ],
        },
        {
          label: 'Engineering',
          collapsed: true,
          items: [
            { label: 'Overview', slug: 'engineering' },
            {
              label: 'Planned Analyst UX',
              collapsed: true,
              items: [
                { label: 'Analyst research experience', slug: 'engineering/analyst-ux' },
                {
                  label: 'Research workspace semantics',
                  slug: 'book/architecture/research-workspaces',
                },
              ],
            },
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
                // BEGIN generated ADR records — scripts/ci/adr-index.py generate
                { label: '0001 — Rust Core', slug: 'adr/0001-rust-core' },
                {
                  label: '0002 — Recursive Descent + Pratt Parser for graphforge-cypher',
                  slug: 'adr/0002-lr1-grammar',
                },
                {
                  label: '0003 — Progressive Ontology — Exploration First',
                  slug: 'adr/0003-progressive-ontology',
                },
                { label: '0004 — Graph-Native Adjacency Index', slug: 'adr/0004-adjacency-index' },
                {
                  label: '0005 — Layered Architecture — Graph / Knowledge / Workbench',
                  slug: 'adr/0005-layered-architecture',
                },
                {
                  label: '0006 — Append-only epistemic interpretation',
                  slug: 'adr/0006-epistemic-model',
                },
                { label: '0007 — Runtime Temporal Values', slug: 'adr/0007-temporal-values' },
                { label: '0008 — Heterogeneous List Values', slug: 'adr/0008-heterogeneous-lists' },
                {
                  label: '0009 — Nested Heterogeneous List Values',
                  slug: 'adr/0009-nested-heterogeneous-lists',
                },
                {
                  label: '0010 — Full-range dates (proleptic-Gregorian calendar) and a wider duration model',
                  slug: 'adr/0010-wide-date-and-duration',
                },
                {
                  label: '0011 — Dynamic Heterogeneous Value Lists',
                  slug: 'adr/0011-dynamic-heterogeneous-values',
                },
                {
                  label: '0012 — Knowledge and epistemic domain ownership and schema evolution',
                  slug: 'adr/0012-knowledge-domain-ownership',
                },
                {
                  label: '0013 — Durable v0.5 project-generation protocol',
                  slug: 'adr/0013-project-generation-protocol',
                },
                {
                  label: '0014 — Complete-workspace checkpoints and generation-preserving revert',
                  slug: 'adr/0014-workspace-checkpoints',
                },
                {
                  label: '0015 — Three embedded project-write modes',
                  slug: 'adr/0015-embedded-write-modes',
                },
                {
                  label: '0016 — Repository integration and deployment configuration boundary',
                  slug: 'adr/0016-repository-integration-and-deployment-configuration',
                },
                {
                  label: '0018 — Acknowledged durability and isolation contract',
                  slug: 'adr/0018-acknowledged-durability-isolation',
                },
                {
                  label: '0019 — Authoritative durable graph delta journal',
                  slug: 'adr/0019-authoritative-graph-delta-journal',
                },
                {
                  label: '0020 — NTFS write-through namespace durability',
                  slug: 'adr/0020-ntfs-write-through-namespace-durability',
                },
                {
                  label: '0021 — Portable project v2 package layout and identity',
                  slug: 'adr/0021-portable-project-v2',
                },
                {
                  label: '0022 — Multi-ontology semantics in portable project v2',
                  slug: 'adr/0022-portable-v2-multi-ontology-compatibility',
                },
                {
                  label: '0023 — Composable ontology modules and semantic bridges',
                  slug: 'adr/0023-composable-multi-ontology',
                },
                {
                  label: '0024 — Storage format exceptions for GFDR and compiled ontologies',
                  slug: 'adr/0024-storage-format-exceptions',
                },
                {
                  label: '0025 — Storage values have a compiler-independent contract',
                  slug: 'adr/0025-storage-value-contract',
                },
                {
                  label: '0026 — Read plans bind resources in execution',
                  slug: 'adr/0026-read-plan-resources',
                },
                {
                  label: '0027 — Native GraphForge execution boundary',
                  slug: 'adr/0027-native-runtime-boundary',
                },
                {
                  label: '0028 — One transaction owns graph mutation effects',
                  slug: 'adr/0028-shared-mutation-transaction',
                },
                {
                  label: '0029 — Compile against immutable schema and catalog data',
                  slug: 'adr/0029-lowering-schema-snapshot',
                },
                {
                  label: '0030 — Portable OCI protocol boundary',
                  slug: 'adr/0030-portable-oci-boundary',
                },
                {
                  label: '0031 — Reviewed source file size bounds',
                  slug: 'adr/0031-source-size-policy',
                },
                {
                  label: '0032 — Research Branches share Project publication authority',
                  slug: 'adr/0032-research-project-authority',
                },
                {
                  label: '0035 — Preserve stage diagnostics at public error boundaries',
                  slug: 'adr/0035-structured-stage-errors',
                },
                {
                  label: '0036 — The GraphForge release version contract',
                  slug: 'adr/0036-release-version-contract',
                },
                {
                  label: '0037 — Derived adjacency is published with the generation',
                  slug: 'adr/0037-adjacency-published-with-generation',
                },
                {
                  label: '0038 — Determinism belongs at the publication boundary',
                  slug: 'adr/0038-determinism-at-the-publication-boundary',
                },
                {
                  label: '0039 — Research Versions share Project publication authority',
                  slug: 'adr/0039-research-version-publication',
                },
                // END generated ADR records
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
