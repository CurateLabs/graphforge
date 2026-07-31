import assert from "node:assert/strict";
import { Writable } from "node:stream";
import test from "node:test";

import { run } from "../lib/run.mjs";

function sink() {
  let output = "";
  const stream = new Writable({
    write(chunk, _encoding, callback) {
      output += chunk.toString();
      callback();
    },
  });
  return { stream, output: () => output };
}

test("forwards arguments to the native CLI contract", async () => {
  const stdout = sink();
  const stderr = sink();
  const calls = [];
  const native = {
    runCli(args) {
      calls.push(args);
      return { exitCode: 0, stdout: '{"valid":true}\n', stderr: "" };
    },
  };

  const exitCode = await run(["--json", "config", "validate"], {
    stdout: stdout.stream,
    stderr: stderr.stream,
    native,
  });

  assert.equal(exitCode, 0);
  assert.deepEqual(calls, [["--json", "config", "validate"]]);
  assert.equal(stdout.output(), '{"valid":true}\n');
  assert.equal(stderr.output(), "");
});

test("preserves native stderr and non-zero exit status", async () => {
  const stdout = sink();
  const stderr = sink();
  const exitCode = await run(["remove"], {
    stdout: stdout.stream,
    stderr: stderr.stream,
    native: {
      runCli: () => ({
        exit_code: 2,
        stdout: Buffer.alloc(0),
        stderr: Buffer.from("confirmation required\n"),
      }),
    },
  });

  assert.equal(exitCode, 2);
  assert.equal(stdout.output(), "");
  assert.equal(stderr.output(), "confirmation required\n");
});

test("rejects invalid adapter inputs and native results", async () => {
  await assert.rejects(() => run([1], { native: {} }), /array of strings/);
  await assert.rejects(
    () => run([], { native: {} }),
    /does not expose the runCli contract/,
  );
  await assert.rejects(
    () => run([], { native: { runCli: () => ({ exitCode: -1 }) } }),
    /invalid CLI exit code/,
  );
});
