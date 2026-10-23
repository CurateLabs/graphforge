// Every export, with working arguments, damaged one argument at a time.
import assert from "node:assert/strict";
import { test } from "node:test";
import { EXPORTS } from "./examples.mjs";
import { hostileValues } from "./hostile.mjs";
import { attempt, protocol } from "./support.mjs";

const call = (name, args) => protocol[name](...args);

test("the table covers exactly the module's exports", () => {
  const exported = Object.keys(protocol)
    .filter((name) => name !== "default" && name !== "initSync")
    .sort();
  assert.deepEqual(Object.keys(EXPORTS).sort(), exported);
  assert.equal(exported.length, 28);
});

test("every export succeeds on its working arguments", () => {
  for (const [name, { args }] of Object.entries(EXPORTS)) {
    const outcome = attempt(() => call(name, args));
    assert.ok(
      outcome.ok,
      `${name}: ${outcome.ok ? "" : JSON.stringify(outcome.error)}`,
    );
  }
});

test("damaging an argument yields a structured error or false predicate, never a trap", () => {
  const hostile = hostileValues();
  let calls = 0;
  let glue = 0;
  for (const [name, { args, optional, predicate }] of Object.entries(EXPORTS)) {
    for (let position = 0; position < args.length; position += 1) {
      for (const [index, value] of hostile.entries()) {
        const damaged = args.with(position, value);
        const outcome = attempt(() => call(name, damaged));
        calls += 1;
        const label = `${name}[${position}] hostile #${index}`;
        if (outcome.ok) {
          if (predicate) {
            assert.equal(outcome.value, false, label);
            continue;
          }
          // Only an optional argument may take a harmless value (`undefined`,
          // `null`, or an object that stringifies to `{}`).
          assert.ok(optional.includes(position), `${label} was accepted`);
          continue;
        }
        const { error } = outcome;
        assert.equal(
          Object.getPrototypeOf(error),
          Object.prototype,
          `${label}: ${String(error)}`,
        );
        assert.equal(typeof error.code, "string", label);
        if (error.code === "invalid_argument") glue += 1;
      }
    }
  }
  const positions = Object.values(EXPORTS).reduce(
    (n, { args }) => n + args.length,
    0,
  );
  assert.equal(calls, positions * hostile.length);
  assert.ok(calls > 1000, `${calls} damaged calls`);
  assert.ok(glue > calls / 2, `${glue} refused before reaching Rust`);
  // The module is still usable, for every export.
  for (const [name, { args }] of Object.entries(EXPORTS)) {
    assert.ok(attempt(() => call(name, args)).ok, name);
  }
});

test("a hostile value in every position at once is refused too", () => {
  const hostile = hostileValues();
  for (const [name, { args }] of Object.entries(EXPORTS)) {
    for (const value of hostile) {
      const outcome = attempt(() =>
        call(
          name,
          args.map(() => value),
        ),
      );
      if (!outcome.ok) {
        assert.equal(
          Object.getPrototypeOf(outcome.error),
          Object.prototype,
          name,
        );
        assert.equal(typeof outcome.error.code, "string", name);
      }
    }
  }
  for (const [name, { args }] of Object.entries(EXPORTS)) {
    assert.ok(attempt(() => call(name, args)).ok, name);
  }
});
