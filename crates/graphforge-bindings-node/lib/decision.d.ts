/** Opaque SHA-256 bytes, represented as 32 JSON numbers in the native request. */
export type Sha256Bytes = readonly number[];

export interface DecisionInputIdentityV1 {
  generation_uuid: string;
  version_uuid?: string | null;
  projection_sha256: Sha256Bytes;
  selection_sha256: Sha256Bytes;
  selected_item_uuids: readonly string[];
}

export interface DecisionProducerV1 {
  name: string;
  model?: string | null;
  revision?: string | null;
}

export type DecisionQuestionKindV1 =
  | { kind: "choice"; allowed_choices: readonly string[] }
  | { kind: "rubric_score"; ordered_levels: readonly string[] }
  | { kind: "yes_no_probability" };

export interface DecisionQuestionV1 {
  question_uuid: string;
  text: string;
  /** Empty means one project level result with a null item_uuid. */
  item_uuids: readonly string[];
  kind: DecisionQuestionKindV1;
}

export type DecisionValueV1 =
  | { kind: "choice"; value: string }
  | { kind: "rubric_score"; value: string }
  | {
      kind: "yes_no_probability";
      value: { yes_probability: number; no_probability?: number | null };
    };

export interface DecisionConfidenceV1 {
  value: number;
  minimum: number;
  maximum: number;
  domain: string;
  meaning: string;
}

export interface DecisionResultV1 {
  question_uuid: string;
  item_uuid?: string | null;
  status: "answered" | "uncertain" | "unavailable" | "missing";
  value?: DecisionValueV1 | null;
  confidence?: DecisionConfidenceV1 | null;
}

export interface DecisionBatchV1 {
  input: DecisionInputIdentityV1;
  producer: DecisionProducerV1;
  questions: readonly DecisionQuestionV1[];
  results: readonly DecisionResultV1[];
}

declare module "../index.js" {
  interface GraphForge {
    /** Validate with Rust and return Arrow IPC bytes using decision_result/1. */
    validateDecisionBatch(request: DecisionBatchV1): Uint8Array;
  }
}
