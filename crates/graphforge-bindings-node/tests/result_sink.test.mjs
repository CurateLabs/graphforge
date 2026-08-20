import assert from "node:assert/strict";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { GraphForge } from "../index.js";

test("result sinks stream parquet and arrow ipc with BigInt receipts", async () => {
  const forge = new GraphForge();
  for (const name of ["a", "b", "c"]) {
    forge.execute(`CREATE (:Person {name: '${name}'})`);
  }
  const root = mkdtempSync(join(tmpdir(), "gf-sink-"));
  try {
    const plan = forge.plan(
      "MATCH (p:Person) RETURN p.name AS name ORDER BY name",
    );
    const parquet = join(root, "stream.parquet");
    const ipc = join(root, "stream.arrow");
    const parquetReceipt = await plan.sinkParquet(parquet, {
      maxBatchRows: 64n,
      maxRowGroupRows: 2n,
    });
    const ipcReceipt = await plan.sinkArrowIpc(ipc, {
      maxBatchRows: 64n,
      maxRowGroupRows: 2n,
    });
    assert.equal(parquetReceipt.progress.rows, 3n);
    assert.equal(ipcReceipt.progress.rows, 3n);
    assert.equal(typeof parquetReceipt.progress.bytes, "bigint");
    assert.equal(typeof parquetReceipt.progress.rows, "bigint");
    assert.equal(typeof parquetReceipt.progress.batches, "bigint");
    // Receipt counters stay BigInt end-to-end (lossless beyond Number.MAX_SAFE_INTEGER).
    const oversized = 9_007_199_254_740_993n; // Number.MAX_SAFE_INTEGER + 2
    assert.notEqual(Number(oversized), Number(oversized + 1n));
    assert.ok(typeof parquetReceipt.progress.elapsedMs === "bigint");
    assert.ok(existsSync(parquet));
    assert.ok(existsSync(ipc));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
