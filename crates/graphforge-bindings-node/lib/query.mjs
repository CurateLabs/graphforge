/** Typed query helper composing `forge.execute()` for first use.
 *
 * Returns an apache-arrow `Table` directly, so reading a useful result needs
 * no Arrow IPC decoding. `forge.execute()` keeps returning raw IPC buffers
 * for code that depends on that form.
 */

import { tableFromIPC } from "apache-arrow";

/**
 * Run one Cypher statement and decode its Arrow IPC result into an
 * apache-arrow `Table`.
 *
 * `params` binds `$name` placeholders exactly as `forge.execute()` does.
 */
export function query(forge, cypher, params) {
  return tableFromIPC(forge.execute(cypher, params));
}
