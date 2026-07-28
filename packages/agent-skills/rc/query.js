import { tableToJson } from "../adapter/index.js";

const BOOTSTRAP_QUERY =
  "MATCH (n:GraphForgeBootstrap {key: 'agent-skills/v1'}) RETURN n.node_uuid AS node_uuid";

export async function readBootstrapMarker(graph, tableFromIPC) {
  const ipc = await Promise.resolve(graph.execute(BOOTSTRAP_QUERY));
  const table = Number.isInteger(ipc?.numRows) ? ipc : tableFromIPC(ipc);
  return tableToJson(table);
}
