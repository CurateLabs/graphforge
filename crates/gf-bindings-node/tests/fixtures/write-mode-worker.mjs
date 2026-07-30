import { parentPort, workerData } from "node:worker_threads";

import { GraphForge } from "../../index.js";

try {
  const forge = new GraphForge(workerData.project, {
    maxRebaseAttempts: 8,
    writeMode: "optimistic_multi_writer",
    writeQueueCapacity: 8,
  });
  const receipt = forge.publishCompositeTransaction({
    contractVersion: 1,
    operationUuid: workerData.operationUuid,
    graphMutations: [
      {
        kind: "create_node",
        label: "Person",
        nodeUuid: workerData.nodeUuid,
        properties: { name: workerData.name },
      },
    ],
  });
  forge.close();
  parentPort.postMessage({
    ok: receipt.byteLength > 0,
    operationUuid: workerData.operationUuid,
  });
} catch (error) {
  parentPort.postMessage({
    code: error?.code,
    message: error instanceof Error ? error.message : String(error),
    ok: false,
    stack: error instanceof Error ? error.stack : undefined,
  });
}
