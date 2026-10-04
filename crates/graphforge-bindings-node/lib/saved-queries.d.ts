/** A project-owned definition. Saving or loading never executes its Cypher. */
export interface SavedQuery {
  query_uuid: string;
  name: string;
  description: string | null;
  query: string;
  parameters: Record<string, "boolean" | "integer" | "float" | "string" | "uuid">;
}

/** Choose current project state or an exact retained research Version. */
export type SavedQuerySource = { kind: "current" } | { kind: "version"; version_uuid: string };

/** UUID parameters use the same explicit tag as ordinary native queries. */
export type SavedQueryParameters = Record<string, boolean | number | string | { $uuid: string }>;
