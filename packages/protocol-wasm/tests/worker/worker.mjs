// A minimal Cloudflare Worker that validates a Hub publication with the module,
// importing it the way the package README documents for Workers.
import wasm from "@curatelabs/graphforge-protocol-wasm/wasm";
import * as protocol from "@curatelabs/graphforge-protocol-wasm";

const exports = protocol.initSync({ module: wasm });

// Workers carry no BigInt in JSON; render each as a tagged string.
const json = (value, status = 200) =>
  new Response(
    JSON.stringify(value, (_key, member) =>
      typeof member === "bigint" ? `${member}n` : member,
    ),
    { status, headers: { "content-type": "application/json" } },
  );

export default {
  async fetch(request) {
    // Rust raises a plain object `{code, path?, version?, message}`.
    try {
      const payload = await request.json();
      if (payload.admission) {
        return json(protocol.plan_item_admission(payload.admission));
      }
      if (payload.admission_query) {
        return json(
          protocol.item_admission_requirements(payload.admission_query),
        );
      }
      const { manifest, refs, summary, lineage, intent, rename } = payload;
      const validated = {
        manifest: protocol.validate_manifest(manifest),
        refs: protocol.validate_refs(refs),
        summary: protocol.validate_summary(summary),
        lineage: protocol.validate_lineage(lineage),
      };
      protocol.bind_summary(manifest, summary);
      protocol.bind_refs(refs, manifest);
      const rehomed = protocol.rehome_repository(
        refs,
        [{ manifest: validated.manifest.document, summary }],
        rename.identity,
        rename.template,
      );
      return json({
        manifest_digest: validated.manifest.digest,
        summary_digest: validated.summary.digest,
        lineage_digest: validated.lineage.digest,
        refs_revision: validated.refs.revision,
        object_lengths: validated.manifest.document.objects.map(
          (o) => o.length,
        ),
        request_commitment: protocol.request_commitment(intent),
        rehomed_revision: rehomed.revision,
        rehomed_manifest: rehomed.refs.refs[0].validator,
        linear_memory_bytes: exports.memory.buffer.byteLength,
      });
    } catch (error) {
      return json(error, 422);
    }
  },
};
