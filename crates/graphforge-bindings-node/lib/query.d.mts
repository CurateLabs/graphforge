import type { Table } from "apache-arrow";

/** Minimal structural view of a GraphForge instance's query surface. */
export interface QueryRunner {
  execute(cypher: string, params?: Record<string, unknown> | null): Uint8Array;
}

/**
 * Run one Cypher statement and decode its Arrow IPC result into an
 * apache-arrow `Table`.
 *
 * `params` binds `$name` placeholders exactly as `GraphForge.execute()` does.
 */
export function query(
  forge: QueryRunner,
  cypher: string,
  params?: Record<string, unknown> | null,
): Table;
