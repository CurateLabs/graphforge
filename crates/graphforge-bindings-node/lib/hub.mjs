/** Typed Hub/research verbs over the in-process native Rust CLI.
 * Close other writable handles before a verb; reopen GraphForge to read the
 * committed data as Arrow. Metadata retains native field names and identities.
 */
import { resolve } from "node:path";
import { runCli } from "../index.js";

/** Native refusal with its original envelope and process-compatible output. */
export class HubWorkflowError extends Error {
  constructor(result) {
    const envelope = JSON.parse(Buffer.from(result.stderr).toString("utf8"));
    super(envelope.error.message);
    this.name = "HubWorkflowError";
    this.envelope = envelope;
    this.nativeCode = envelope.error.code;
    this.details = envelope.error.details ?? {};
    this.code = this.details.semantic_code ?? this.nativeCode;
    this.exitCode = result.exitCode;
    this.stdout = result.stdout;
    this.stderr = result.stderr;
  }
}

function options(values) {
  return Object.entries(values).flatMap(([key, value]) => {
    const flag = `--${key.replaceAll("_", "-")}`;
    if (value === true) return [flag];
    return value == null || value === false ? [] : [`${flag}=${value}`];
  });
}

/** Project path plus credential-source names; Rust owns all workflow behavior. */
export class HubProject {
  constructor(path, { hub, tokenEnv, protectionBypassEnv } = {}) {
    this.path = resolve(path);
    this.warnings = [];
    this._hub = options({
      hub,
      token_env: tokenEnv,
      protection_bypass_env: protectionBypassEnv,
    });
  }

  _run(verb, args, remote = false) {
    const result = runCli([
      "--json",
      `--project=${this.path}`,
      verb,
      ...(remote ? this._hub : []),
      ...args,
    ]);
    if (result.exitCode !== 0) {
      const error = new HubWorkflowError(result);
      this.warnings = error.envelope.warnings ?? [];
      throw error;
    }
    const value = JSON.parse(result.stdout.toString("utf8"));
    this.warnings = value.warnings ?? [];
    // Native warnings wrap array results; the typed log remains an array.
    return verb === "log" && !Array.isArray(value) ? value.result : value;
  }

  clone(repository, { ref, tag, versionUuid } = {}) {
    return this._run(
      "clone",
      [
        ...options({ ref, tag, version_uuid: versionUuid }),
        "--",
        repository,
        this.path,
      ],
      true,
    );
  }

  branch(name, { author, committer, fromVersion, branch, operationUuid } = {}) {
    return this._run("branch", [
      ...options({
        author,
        committer,
        from: fromVersion,
        branch,
        operation_uuid: operationUuid,
      }),
      "--",
      name,
    ]);
  }

  commit(message, query, { author, committer, branch, operationUuid } = {}) {
    return this._run(
      "commit",
      options({
        message,
        query,
        author,
        committer,
        branch,
        operation_uuid: operationUuid,
      }),
    );
  }

  tag(name, version, { branch, operationUuid } = {}) {
    return this._run("tag", [
      ...options({ branch, operation_uuid: operationUuid }),
      "--",
      name,
      ...(version == null ? [] : [version]),
    ]);
  }

  push(
    repository,
    {
      ref,
      branch,
      rewrite = false,
      prune = false,
      operationUuid,
      stagingDirectory,
    } = {},
  ) {
    return this._run(
      "push",
      [
        ...options({
          ref,
          branch,
          rewrite,
          prune,
          operation_uuid: operationUuid,
          staging_directory: stagingDirectory,
        }),
        ...(repository == null ? [] : ["--", repository]),
      ],
      true,
    );
  }

  publish(repository, options) {
    return this.push(repository, options);
  }

  pull(repository, { ref, branch, author, committer, operationUuid } = {}) {
    return this._run(
      "pull",
      [
        ...options({
          ref,
          branch,
          author,
          committer,
          operation_uuid: operationUuid,
        }),
        ...(repository == null ? [] : ["--", repository]),
      ],
      true,
    );
  }

  log(version, { branch, remote = false, ref, maxCount, all = false } = {}) {
    return this._run(
      "log",
      [
        ...options({ branch, ref, max_count: maxCount, all }),
        ...(remote === false
          ? []
          : [`--remote=${remote === true ? "" : remote}`]),
        ...(version == null ? [] : ["--", version]),
      ],
      remote !== false,
    );
  }
}
