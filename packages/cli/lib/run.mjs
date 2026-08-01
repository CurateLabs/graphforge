import { fileURLToPath } from "node:url";

const packagedSkills = fileURLToPath(
  new URL("../project-skills/", import.meta.url),
);

/**
 * Forward one invocation to the Rust-owned CLI contract.
 *
 * `native` is injectable for contract tests; production callers always use the
 * public `@curatelabs/graphforge` binding.
 */
export async function run(
  args,
  { stdout = process.stdout, stderr = process.stderr, native } = {},
) {
  if (!Array.isArray(args) || args.some((arg) => typeof arg !== "string")) {
    throw new TypeError("GraphForge CLI arguments must be an array of strings");
  }

  const binding = native ?? (await import("@curatelabs/graphforge"));
  if (typeof binding.runCli !== "function") {
    throw new TypeError(
      "@curatelabs/graphforge does not expose the runCli contract",
    );
  }

  const result = await binding.runCli([
    "--skills-bundle-dir",
    packagedSkills,
    ...args,
  ]);
  const exitCode = result.exitCode ?? result.exit_code;
  if (!Number.isInteger(exitCode) || exitCode < 0 || exitCode > 255) {
    throw new TypeError(
      "@curatelabs/graphforge returned an invalid CLI exit code",
    );
  }

  write(stdout, result.stdout);
  write(stderr, result.stderr);
  return exitCode;
}

function write(stream, value) {
  if (value === undefined || value === null) return;
  if (typeof value !== "string" && !ArrayBuffer.isView(value)) {
    throw new TypeError("@curatelabs/graphforge returned invalid CLI output");
  }
  if (value.length > 0) stream.write(value);
}
