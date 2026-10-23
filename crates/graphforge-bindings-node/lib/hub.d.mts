/** Control metadata retains the field names of the native CLI contract. */
export interface CoreVersionWarning {
  code: string;
  producing_core_version: string;
  running_core_version: string;
  message: string;
}
export interface CloneResult {
  contract: string;
  repository: string;
  destination: string;
  immutable_version: string;
  package_digest: string;
  generation_uuid: string;
  resumed_bytes: number;
  resolved_ref?: string;
  research_branch_uuid?: string;
  research_version_uuid?: string;
  research_version_kind?: string;
  research_version_identity?: string;
  moved_from?: string;
  moved_to?: string;
  warnings?: CoreVersionWarning[];
}
export interface PublishResult {
  format: string;
  operation_uuid: string;
  request_commitment: string;
  repository: { owner: string; repository: string };
  manifest_validator: string;
  refs: string[];
  previous_revision: string | null;
  revision: string;
  moved_from?: string;
  moved_to?: string;
  warnings?: CoreVersionWarning[];
}
export interface OperationResult {
  operation_uuid: string;
  generation_uuid: string;
  version_uuid: string | null;
  replayed: boolean;
}
export interface CommitResult extends OperationResult {
  parents: string[];
}
export interface BranchResult extends OperationResult {
  label: string;
}
export interface TagResult extends OperationResult {
  name: string;
}
export interface PullResult extends CommitResult {
  incorporated: number;
  conflicts: string[];
}
export interface UpToDateResult {
  up_to_date: boolean;
  branch_uuid: string;
  head_version_uuid: string;
}
export interface Signature {
  name: string;
  email?: string;
  orcid?: string;
}
export interface LogEntry {
  version_uuid: string;
  parents: string[];
  author: Signature | null;
  committer: Signature | null;
  created_at: number | null;
  label: string | null;
  description: string | null;
  tags: string[];
  kind: string;
}
export interface HubOptions {
  hub?: string;
  tokenEnv?: string;
  protectionBypassEnv?: string;
}
export interface OperationOptions {
  branch?: string;
  operationUuid?: string;
}
export interface CreditOptions extends OperationOptions {
  author?: string;
  committer?: string;
}
export interface PushOptions extends OperationOptions {
  ref?: string;
  rewrite?: boolean;
  prune?: boolean;
  stagingDirectory?: string;
}
export interface NativeErrorEnvelope {
  error: {
    code: string;
    message: string;
    details?: Record<string, unknown>;
    diagnostics?: unknown[];
  };
  [key: string]: unknown;
}
/** code = details.semantic_code when present, otherwise the native broad code. */
export class HubWorkflowError extends Error {
  readonly code: string;
  readonly nativeCode: string;
  readonly details: Record<string, unknown>;
  readonly envelope: NativeErrorEnvelope;
  readonly exitCode: number;
  readonly stdout: Uint8Array;
  readonly stderr: Uint8Array;
  constructor(result: {
    exitCode: number;
    stdout: Uint8Array;
    stderr: Uint8Array;
  });
}
/** Synchronous native workflow; each verb opens the durable project afresh. */
export class HubProject {
  readonly path: string;
  readonly warnings: CoreVersionWarning[];
  constructor(path: string, options?: HubOptions);
  clone(
    repository: string,
    options?: { ref?: string; tag?: string; versionUuid?: string },
  ): CloneResult;
  branch(
    name: string,
    options?: CreditOptions & { fromVersion?: string },
  ): BranchResult;
  commit(message: string, query: string, options?: CreditOptions): CommitResult;
  tag(name: string, version?: string, options?: OperationOptions): TagResult;
  push(repository?: string, options?: PushOptions): PublishResult;
  publish(repository?: string, options?: PushOptions): PublishResult;
  pull(
    repository?: string,
    options?: CreditOptions & { ref?: string },
  ): PullResult | UpToDateResult;
  log(
    version?: string,
    options?: {
      branch?: string;
      remote?: boolean | string;
      ref?: string;
      maxCount?: number;
      all?: boolean;
    },
  ): LogEntry[];
}
