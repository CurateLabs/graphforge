// Research Project metadata and bounded local discovery (#1348).

import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";

const require = createRequire(import.meta.url);
let { GraphForge } = require("../index.js");
if (typeof GraphForge.prototype.researchProjectMetadata !== "function") {
  ({ GraphForge } = require("../graphforge.node"));
}

const operation = (suffix) =>
  `018f0f4e-7b8c-7000-8000-${suffix.toString().padStart(12, "0")}`;

function openAdmittedProject(path) {
  try {
    return new GraphForge(path);
  } catch (error) {
    if (error.code === "GF_UNSUPPORTED_FILESYSTEM") {
      return null;
    }
    throw error;
  }
}

test("research metadata and discovery stay metadata-only across reopen", () => {
  const firstRoot = mkdtempSync(join(tmpdir(), "gf-research-"));
  const secondRoot = mkdtempSync(join(tmpdir(), "gf-research-"));
  try {
    if (!openAdmittedProject(firstRoot) || !openAdmittedProject(secondRoot)) {
      return;
    }

    const first = new GraphForge(firstRoot);
    first.updateResearchMetadata({
      metadata: {
        contract_version: 1,
        title: "Arabian Nights",
        description: null,
        authors: [],
        subjects: ["literature"],
        languages: ["ar"],
        geographic_coverage: null,
        temporal_coverage: {
          start: "800",
          end: "1500",
          label: "800-1500 CE",
        },
        source_types: [],
        corpus_size: null,
        ontologies: ["narrative-events"],
        license: null,
        access: {
          visibility: null,
          access_policy: null,
          collaborators: [],
        },
        tags: [],
        originating_projects: [],
        related_projects: [],
        canonical_identifiers: [],
        external_identifiers: [],
        created_at: null,
        updated_at: null,
        extensions: {},
        discovery_facets: {
          text_entry_points: 0,
          source_entry_points: 0,
          entity_entry_points: 0,
          relationship_entry_points: 0,
          story_document_entry_points: 0,
          event_location_entry_points: 0,
          ontology_type_entry_points: 0,
          linguistic_property_entry_points: 0,
          traversal_query_entry_points: 0,
          branch_entry_points: 0,
        },
      },
      operationUuid: operation(1),
    });

    const reopened = new GraphForge(firstRoot);
    assert.equal(reopened.researchProjectMetadata().title, "Arabian Nights");
    assert.equal(
      reopened.researchProjectSummary().metadata.title,
      "Arabian Nights",
    );

    const second = new GraphForge(secondRoot);
    second.updateResearchMetadata({
      metadata: {
        contract_version: 1,
        title: "Medieval Latin Corpus",
        description: null,
        authors: [],
        subjects: ["history"],
        languages: ["la"],
        geographic_coverage: null,
        temporal_coverage: null,
        source_types: [],
        corpus_size: null,
        ontologies: [],
        license: null,
        access: {
          visibility: null,
          access_policy: null,
          collaborators: [],
        },
        tags: [],
        originating_projects: [],
        related_projects: [],
        canonical_identifiers: [],
        external_identifiers: [],
        created_at: null,
        updated_at: null,
        extensions: {},
        discovery_facets: {
          text_entry_points: 0,
          source_entry_points: 0,
          entity_entry_points: 0,
          relationship_entry_points: 0,
          story_document_entry_points: 0,
          event_location_entry_points: 0,
          ontology_type_entry_points: 0,
          linguistic_property_entry_points: 0,
          traversal_query_entry_points: 0,
          branch_entry_points: 0,
        },
      },
      operationUuid: operation(2),
    });

    const discovered = tableFromIPC(
      GraphForge.discoverResearchProjects({
        projectRoots: [firstRoot, secondRoot],
        query: {
          languages: ["ar"],
          subjects: ["literature"],
          ontologies: ["narrative-events"],
          temporalLabel: "800-1500",
        },
      }),
    );
    assert.equal(discovered.numRows, 1);
    assert.equal(discovered.getChild("title").get(0), "Arabian Nights");
  } finally {
    rmSync(firstRoot, { recursive: true, force: true });
    rmSync(secondRoot, { recursive: true, force: true });
  }
});
