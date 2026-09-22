// Real two-story journey: every research decision executes in native Rust.
import assert from "node:assert/strict";
import { readFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const corpus = JSON.parse(
  readFileSync(
    new URL(
      "../../../tests/fixtures/analyst-journey-v1/corpus.json",
      import.meta.url,
    ),
    "utf8",
  ),
);
let sequence = 0;
const identity = () =>
  `018f0f4e-7b8c-7000-8000-${(++sequence).toString(16).padStart(12, "0")}`;
function uuid(value) {
  if (typeof value === "string") return value;
  const hex = Buffer.from(value).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
const rows = (bytes) =>
  tableFromIPC(bytes)
    .toArray()
    .map((row) => row.toJSON());
const scalar = (bytes) => tableFromIPC(bytes).getChildAt(0).get(0);

class Journey {
  constructor(root) {
    this.root = root;
    this.graph = new GraphForge(join(root, "project"));
    this.actor = identity();
  }
  current() {
    return this.graph.researchProjectSummary().identity.generationUuid;
  }
  async setup() {
    await this.graph.execute(corpus.initial_graph);
    this.ids = Object.fromEntries(
      rows(
        await this.graph.execute(
          "MATCH(n) RETURN n.name AS name,n.node_uuid AS id",
        ),
      ).map((row) => [row.name, uuid(row.id)]),
    );
    const metadata = this.graph.researchProjectMetadata();
    metadata.title = corpus.title;
    this.graph.updateResearchMetadata({ metadata, operationUuid: identity() });
    const before = this.current();
    const discovery = tableFromIPC(
      GraphForge.discoverResearchProjects({
        projectRoots: [join(this.root, "project")],
      }),
    );
    assert.deepEqual(Array.from(discovery.getChild("title")), [corpus.title]);
    assert.equal(this.current(), before);
  }
  async evidence() {
    for (const capabilityId of ["provenance", "knowledge", "epistemic"]) {
      await this.graph.enableCapability({
        operationUuid: identity(),
        capabilityId,
        capabilityVersion: 1,
      });
    }
    [this.source, this.scan, this.ocr, this.claim] = Array.from(
      { length: 4 },
      identity,
    );
    await this.graph.registerSource({
      operationUuid: identity(),
      sourceUuid: this.source,
      label: corpus.source_label,
      sourceKind: "manuscript",
    });
    for (const [artifactUuid, artifactKind, text, derivationInputs] of [
      [this.scan, "raw_scan", corpus.scan_bytes, []],
      [
        this.ocr,
        "ocr_text",
        corpus.ocr_text,
        [{ inputUuid: this.scan, inputKind: "artifact" }],
      ],
    ]) {
      await this.graph.registerArtifact({
        operationUuid: identity(),
        artifactUuid,
        sourceUuid: this.source,
        artifactKind,
        mediaType: "text/plain",
        payload: { kind: "local_bytes", bytes: Buffer.from(text) },
        derivationInputs,
      });
    }
    const artifactCursor = await this.inspectEvidence();
    await this.graph.setPreferredArtifact({
      operationUuid: identity(),
      preferenceEventUuid: identity(),
      sourceUuid: this.source,
      artifactUuid: this.ocr,
      reason: "Selected transcription",
    });
    const afterPreference = this.current();
    await assert.rejects(
      this.graph.listArtifacts({
        sourceUuid: this.source,
        limit: 1,
        after: artifactCursor,
      }),
      (error) => error.code === "GF_PAGE_SNAPSHOT_GONE",
    );
    assert.equal(this.current(), afterPreference);
    await this.graph.createResearchClaim({
      operation_uuid: identity(),
      expected_generation_uuid: this.current(),
      assertion_uuid: this.claim,
      claim: corpus.claim,
      graph_refs: [
        {
          graph_uuid: this.ids.Ada,
          graph_kind: "node",
          role: "subject",
          ordinal: 0,
        },
      ],
      category: "analyst_assertion",
      creator_uuid: this.actor,
      run_uuid: null,
      created_at: 1,
    });
    await this.graph.attachEvidence({
      operationUuid: identity(),
      evidenceUuid: identity(),
      assertionUuid: this.claim,
      sourceUuid: this.ocr,
      sourceKind: "artifact",
      role: "supports",
    });
    const lineage = rows(
      await this.graph.researchLineage(this.ocr, {
        subjectKind: "artifact",
        direction: "backward",
        maxDepth: 4,
      }),
    );
    assert.ok(lineage.some((row) => uuid(row.input_uuid) === this.scan));
  }
  async inspectEvidence() {
    const before = this.current();
    const source = rows(await this.graph.source(this.source));
    assert.equal(source.length, 1);
    assert.equal(uuid(source[0].source_uuid), this.source);
    assert.equal(source[0].label, corpus.source_label);
    assert.equal(source[0].source_kind, "manuscript");
    assert.deepEqual(rows(await this.graph.listSources()), source);

    const artifacts = [];
    for (const [id, kind, text] of [
      [this.scan, "raw_scan", corpus.scan_bytes],
      [this.ocr, "ocr_text", corpus.ocr_text],
    ]) {
      const found = rows(await this.graph.artifact(id));
      assert.equal(found.length, 1);
      assert.equal(uuid(found[0].artifact_uuid), id);
      assert.equal(uuid(found[0].source_uuid), this.source);
      assert.equal(found[0].artifact_kind, kind);
      assert.equal(found[0].availability, "local_verified");
      assert.equal(found[0].payload_kind, "local_sha256");
      assert.equal(found[0].content_length, BigInt(Buffer.byteLength(text)));
      artifacts.push(found[0]);
    }
    const first = tableFromIPC(
      await this.graph.listArtifacts({ sourceUuid: this.source, limit: 1 }),
    );
    assert.equal(first.numRows, 1);
    const after = first.schema.metadata.get("graphforge.next_page_token");
    assert.ok(after);
    const second = tableFromIPC(
      await this.graph.listArtifacts({
        sourceUuid: this.source,
        limit: 1,
        after,
      }),
    );
    assert.equal(second.numRows, 1);
    assert.equal(
      second.schema.metadata.has("graphforge.next_page_token"),
      false,
    );
    const pages = [...first.toArray(), ...second.toArray()].map((row) =>
      row.toJSON(),
    );
    assert.equal(new Set(pages.map((row) => uuid(row.artifact_uuid))).size, 2);
    assert.deepEqual(pages, artifacts);
    assert.equal(
      tableFromIPC(await this.graph.listArtifacts({ sourceUuid: identity() }))
        .numRows,
      0,
    );
    assert.equal(this.current(), before);
    return after;
  }
  async capture() {
    const version = identity();
    const operation = this.graph.prepareResearchVersion({
      operation_uuid: identity(),
      version_uuid: version,
      context_uuid: identity(),
      label: null,
      description: null,
      created_at: 2,
      required_versions: [],
    });
    await this.graph.commitResearchVersionOperation(operation);
    return version;
  }
  async branches() {
    const source = await this.capture();
    this.a = identity();
    this.b = identity();
    for (const [branch, story, outside] of [
      [this.a, "Mystery", "Voyage"],
      [this.b, "Voyage", "Mystery"],
    ]) {
      const request = {
        request_uuid: identity(),
        source: { kind: "version", version_uuid: source },
        selector: {
          kind: "traverse",
          seeds: [this.ids[story]],
          direction: "both",
          max_depth: 1,
          relationship_types: [],
        },
        include: { assertions: [this.claim] },
      };
      const before = this.current();
      const included = rows(await this.graph.previewSlice(request, "included"));
      assert.deepEqual(
        new Set(
          included
            .filter((r) => r.object_kind === "node")
            .map((r) => r.object_uuid),
        ),
        new Set([this.ids[story], this.ids.Ada]),
      );
      const boundary = rows(await this.graph.previewSlice(request, "boundary"));
      assert.ok(boundary.some((r) => r.object_uuid === this.ids[outside]));
      assert.equal(
        tableFromIPC(await this.graph.previewSlice(request, "explanations"))
          .numRows,
        included.length,
      );
      const why = rows(
        await this.graph.previewSlice(request, "explanations"),
      ).find((r) => r.object_uuid === this.ids.Ada);
      assert.deepEqual(
        [why.reason, why.root_uuid, why.predecessor_uuid, why.depth],
        ["traversal", this.ids[story], this.ids[story], 1],
      );
      assert.ok(why.via_edge_uuid);
      const dependencies = rows(
        await this.graph.previewSlice(request, "dependencies"),
      );
      for (const id of [this.source, this.scan, this.ocr])
        assert.ok(dependencies.some((r) => r.object_uuid === id));
      const frozen = Array.from(await this.graph.freezeSlice(request));
      assert.equal(this.current(), before);
      await this.graph.createResearchBranch({
        operation_uuid: identity(),
        expected_generation_uuid: before,
        branch_uuid: branch,
        version_uuid: identity(),
        source: { kind: "slice", frozen_ipc: frozen },
        creator_uuid: this.actor,
        created_at: 3,
        label: story,
      });
    }
    assert.deepEqual(
      rows(
        await this.graph.queryResearchBranch(
          this.a,
          "MATCH(s:Story)-[:FEATURES]->(c:Character) RETURN s.name AS story,c.name AS character",
        ),
      ),
      [{ story: "Mystery", character: "Ada" }],
    );
  }
  async edit(branch, query) {
    const version = identity();
    await this.graph.executeResearchBranch({
      operation_uuid: identity(),
      expected_generation_uuid: this.current(),
      branch_uuid: branch,
      version_uuid: version,
      created_at: 4,
      query,
    });
    return version;
  }
  async upstream() {
    await this.edit(this.a, corpus.local_edit);
    assert.equal(
      scalar(await this.graph.execute("MATCH(n:Character) RETURN n.score")),
      0n,
    );
    await this.graph.execute(corpus.upstream_edit);
    const request = { branch_uuid: this.a, scope: { kind: "branch" } };
    const before = this.current();
    const preview = tableFromIPC(
      await this.graph.previewResearchUpstream(request),
    );
    assert.equal(this.current(), before);
    const row = preview.toArray().find((r) => r.field === "property:x");
    const version = identity();
    await this.graph.updateResearchBranch({
      operation_uuid: identity(),
      expected_generation_uuid: before,
      version_uuid: version,
      preview: request,
      preview_sha256: Array.from(
        Buffer.from(
          preview.schema.metadata.get("graphforge.upstream.preview_sha256"),
          "hex",
        ),
      ),
      selection: {
        kind: "selected",
        decisions: [
          {
            unit: {
              object_kind: row.object_kind,
              object_uuid: uuid(row.object_uuid),
              field: row.field,
            },
            resolution: { kind: "adopt_upstream" },
          },
        ],
      },
      acknowledge_evidence: [],
      actor_uuid: this.actor,
      created_at: 5,
      explanation: "Only reviewed x",
    });
    const result = rows(
      await this.graph.queryResearchBranch(
        this.a,
        "MATCH(n:Character) RETURN n.score AS score,n.ending AS ending,n.x AS x,n.y AS y",
      ),
    )[0];
    assert.deepEqual(
      {
        ...result,
        score: Number(result.score),
        x: Number(result.x),
        y: Number(result.y),
      },
      corpus.expected_branch_after_upstream,
    );
    return version;
  }
  async nodeCapsule(version) {
    return Array.from(
      await this.graph.freezeSlice({
        request_uuid: identity(),
        source: { kind: "version", version_uuid: version },
        selector: { kind: "direct", members: { nodes: [this.ids.Ada] } },
      }),
    );
  }
  async proposal(version, fields) {
    const proposal = identity();
    await this.graph.submitResearchProposal({
      operation_uuid: identity(),
      expected_generation_uuid: this.current(),
      proposal_uuid: proposal,
      source_branch_uuid: this.a,
      source_version_uuid: version,
      frozen_ipc: await this.nodeCapsule(version),
      fields: fields.map((field) => ({
        object_kind: "node",
        object_uuid: this.ids.Ada,
        field,
      })),
      actor_uuid: this.actor,
      created_at: 6,
      motivation: "Selected public observations",
      policy: "",
    });
    const before = this.current();
    const preview = rows(
      await this.graph.previewResearchProposal({ proposal_uuid: proposal }),
    );
    assert.equal(this.current(), before);
    assert.deepEqual(new Set(preview.map((r) => r.field)), new Set(fields));
    const rendered = JSON.stringify(preview, (_, v) =>
      typeof v === "bigint" ? v.toString() : v,
    );
    assert.ok(
      !rendered.includes("PRIVATE_LOCAL") && !rendered.includes("private_note"),
    );
    return {
      operation_uuid: identity(),
      expected_generation_uuid: before,
      proposal_uuid: proposal,
      preview_sha256: Array.from(Buffer.from(preview[0].preview_sha256, "hex")),
      decisions: Object.fromEntries(
        preview.map((r) => [
          r.item_uuid,
          r.field === "property:ending" ? "defer" : "accept",
        ]),
      ),
      resolve_conflicts: [],
      acknowledge_evidence: [],
      promotions: [],
      community_uuid: null,
      actor_uuid: this.actor,
      created_at: 7,
      explanation: "Accept score, defer ending",
      policy: "",
    };
  }
  async acceptance(version) {
    const before = this.current();
    const changes = rows(
      await this.graph.compareResearch({
        left: { kind: "branch", branch_uuid: this.a },
        right: { kind: "project" },
        left_authority: null,
        right_authority: null,
        detail: "changes",
        accepted: [],
        max_fields: 40000,
        max_bytes: 67108864,
        page_size: 1000,
        after: null,
      }),
    );
    for (const field of corpus.selected_fields)
      assert.ok(changes.some((r) => r.field === field));
    assert.equal(this.current(), before);
    const review = await this.proposal(version, corpus.selected_fields);
    const receipt = await this.graph.reviewResearchProposal(review);
    assert.deepEqual(await this.graph.reviewResearchProposal(review), receipt);
    const result = rows(
      await this.graph.execute(
        "MATCH(n:Character) RETURN n.score AS score,n.ending AS ending,n.private_note AS private_note",
      ),
    )[0];
    assert.deepEqual(
      { ...result, score: Number(result.score) },
      corpus.expected_parent_after_acceptance,
    );
    assert.equal(
      tableFromIPC(
        await this.graph.researchCanonicalChoices({
          context: { kind: "project" },
          community_uuid: null,
        }),
      ).numRows,
      0,
    );
    assert.equal(
      tableFromIPC(
        await this.graph.researchProposalHistory({
          proposal_uuid: review.proposal_uuid,
          detail: "accepted",
          page_size: 100,
        }),
      ).numRows,
      1,
    );
    return [review, receipt];
  }
  async restore(version, review, receipt) {
    const target = { kind: "version", version_uuid: version };
    const citation = await this.graph.researchReference(target);
    const continued = await this.edit(
      this.a,
      "MATCH(n:Character) SET n.score=2",
    );
    assert.equal(
      (
        await this.graph.researchReference({
          kind: "branch",
          branch_uuid: this.a,
        })
      ).version.version_uuid,
      continued,
    );
    assert.deepEqual(
      (await this.graph.researchReference(target)).version,
      citation.version,
    );
    assert.equal(
      scalar(
        this.graph.queryResearchVersion(
          version,
          "MATCH(n:Character) RETURN n.score",
        ),
      ),
      1n,
    );
    await this.edit(this.b, "MATCH(n:Character) SET n.score=73");
    await this.graph.execute("MATCH(n:Character) SET n.score=99");
    const restored = identity();
    await this.graph.restoreResearchBranch({
      operation_uuid: identity(),
      expected_generation_uuid: this.current(),
      branch_uuid: this.a,
      source_version_uuid: version,
      version_uuid: restored,
      created_at: 8,
    });
    assert.deepEqual(await this.graph.reviewResearchProposal(review), receipt);
    const again = await this.graph.reviewResearchProposal(
      await this.proposal(restored, ["property:score"]),
    );
    assert.equal(again.version_uuid, null);
    for (const [branch, score] of [
      [this.a, 1n],
      [this.b, 73n],
    ])
      assert.equal(
        scalar(
          await this.graph.queryResearchBranch(
            branch,
            "MATCH(n:Character) RETURN n.score",
          ),
        ),
        score,
      );
    assert.equal(
      scalar(await this.graph.execute("MATCH(n:Character) RETURN n.score")),
      99n,
    );
    return restored;
  }
  async roundtrip(version, projection = null, name = "complete") {
    const input = join(this.root, name),
      projectRoot = join(this.root, `${name}-import`);
    const exported = await this.graph.exportResearch({
      version_uuid: version,
      output: input,
      bundled: false,
      projection,
    });
    const verified = await GraphForge.verifyPortableV2({ input, mode: "full" });
    assert.equal(verified.packageDigest, exported.package_digest);
    assert.equal(verified.researchInterchange, true);
    const request = { projectRoot, input, operationId: identity() };
    const first = await GraphForge.importPortableV2(request),
      replay = await GraphForge.importPortableV2(request);
    assert.equal(replay.idempotentReplay, true);
    assert.equal(replay.generationUuid, first.generationUuid);
    return new GraphForge(projectRoot);
  }
  async interchange(version) {
    const citation = await this.graph.researchReference({
      kind: "version",
      version_uuid: version,
    });
    const complete = await this.roundtrip(version);
    try {
      assert.deepEqual(complete.researchVersion(version), citation.version);
      assert.deepEqual(
        Buffer.from(
          scalar(complete.researchVersionArtifactPayload(version, this.ocr)),
        ),
        Buffer.from(corpus.ocr_text),
      );
      assert.deepEqual(
        complete.researchVersionOntology(version),
        this.graph.researchVersionOntology(version),
      );
    } finally {
      complete.close();
    }
    const projected = identity();
    const projection = {
      version_uuid: projected,
      frozen_ipc: await this.nodeCapsule(version),
      fields: ["$object", "$labels", "property:score"].map((field) => ({
        object_kind: "node",
        object_uuid: this.ids.Ada,
        field,
      })),
      created_at: 9,
    };
    const selected = await this.roundtrip(version, projection, "selected");
    try {
      const ref = await selected.researchReference({
        kind: "version",
        version_uuid: projected,
      });
      assert.equal(ref.version.content.source_version, version);
      assert.deepEqual(
        rows(
          selected.queryResearchVersion(
            projected,
            "MATCH(n:Character) RETURN n.score AS score,n.private_note AS private_note",
          ),
        ),
        [{ score: 1n, private_note: null }],
      );
    } finally {
      selected.close();
    }
    const metadata = this.graph.researchProjectMetadata();
    metadata.title = corpus.fork_title;
    metadata.access.access_policy = "Independent local governance";
    const request = {
      operation_uuid: identity(),
      project_uuid: identity(),
      version_uuid: version,
      projection: null,
      target: join(this.root, "fork"),
      actor_uuid: this.actor,
      governance: "Independent research",
      adopt_selected_ontology: true,
      metadata,
    };
    const first = await this.graph.forkResearch(request),
      replay = await this.graph.forkResearch(request);
    assert.equal(replay.idempotent_replay, true);
    assert.equal(replay.generation_uuid, first.generation_uuid);
    const fork = new GraphForge(request.target);
    try {
      const ref = await fork.researchReference({
        kind: "version",
        version_uuid: version,
      });
      assert.equal(ref.project_uuid, request.project_uuid);
      assert.notEqual(ref.project_uuid, citation.project_uuid);
      assert.equal(ref.origin_project_uuid, citation.origin_project_uuid);
      assert.deepEqual(fork.researchProjectMetadata(), metadata);
    } finally {
      fork.close();
    }
  }
}

test("native two-story journey preserves evidence scope review restore and interchange", async () => {
  const root = mkdtempSync(join(tmpdir(), "gf-research-journey-"));
  const journey = new Journey(root);
  try {
    await journey.setup();
    await journey.evidence();
    await journey.branches();
    const version = await journey.upstream();
    const [review, receipt] = await journey.acceptance(version);
    const restored = await journey.restore(version, review, receipt);
    await journey.interchange(restored);
    // Run native compaction and cleanup before reopening the durable Project.
    await journey.graph.commitResearchVersionOperation({
      operation_uuid: identity(),
      expected_generation_uuid: journey.current(),
      mutation: { operation: "compact", versions: [restored] },
    });
    const cleanup = journey.graph.executeProjectCleanup({
      retainedAncestors: 0,
    });
    assert.equal(cleanup.dryRun, false);
    assert.equal(cleanup.graphObjectSweep.disposition, "completed");
    assert.ok(cleanup.removed > 0n);
    const before = journey.current(),
      controller = new AbortController();
    controller.abort();
    await assert.rejects(
      journey.graph.researchReference(
        { kind: "version", version_uuid: restored },
        controller.signal,
      ),
      (e) => e.code === "GF_CANCELLED",
    );
    assert.equal(journey.current(), before);
    journey.graph.close();
    journey.graph = new GraphForge(join(root, "project"));
    assert.deepEqual(
      await journey.graph.reviewResearchProposal(review),
      receipt,
    );
    assert.deepEqual(
      Buffer.from(
        scalar(
          journey.graph.researchVersionArtifactPayload(restored, journey.ocr),
        ),
      ),
      Buffer.from(corpus.ocr_text),
    );
    for (const [branch, expected] of [
      [journey.a, 1n],
      [journey.b, 73n],
    ]) {
      assert.equal(
        scalar(
          await journey.graph.queryResearchBranch(
            branch,
            "MATCH(n:Character) RETURN n.score",
          ),
        ),
        expected,
      );
    }
    assert.equal(
      scalar(await journey.graph.execute("MATCH(n:Character) RETURN n.score")),
      99n,
    );
  } finally {
    journey.graph.close();
    rmSync(root, { recursive: true, force: true });
  }
});
