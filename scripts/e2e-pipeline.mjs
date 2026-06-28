import assert from "node:assert/strict";
import net from "node:net";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { DedupeGateway } from "@causal-order/dedupe";
import { orderEvents } from "causal-order";
import WebSocket from "ws";

const TEST_TIMEOUT_MS = 10_000;
const NODE_IDS = ["edge-a", "edge-b", "edge-c"];
const BASE_TIMESTAMP_MS = 1_781_000_000_000;

async function getAvailablePort() {
  const server = net.createServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("Failed to allocate a local TCP port");
  }

  const { port } = address;
  await new Promise((resolveClose) => server.close(resolveClose));
  return port;
}

async function waitFor(predicate, timeoutMs, label) {
  const startedAt = Date.now();
  while (!predicate()) {
    if (Date.now() - startedAt > timeoutMs) {
      throw new Error(`Timed out waiting for ${label}`);
    }
    await delay(25);
  }
}

function buildUniqueMessages() {
  return NODE_IDS.flatMap((nodeId, nodeIndex) =>
    [1, 2, 3, 4].map((sequence) => ({
      nodeId,
      sequence,
      ts: BASE_TIMESTAMP_MS + nodeIndex * 100 + sequence,
      payload: {
        reading: nodeIndex * 10 + sequence,
      },
    })),
  );
}

function withDuplicates(messages) {
  const duplicates = [
    messages[1],
    messages[5],
    messages[10],
  ].map((message) => ({
    ...message,
    payload: { ...message.payload },
  }));

  return [
    messages[4],
    duplicates[0],
    messages[0],
    messages[8],
    messages[1],
    messages[9],
    messages[5],
    duplicates[1],
    messages[2],
    messages[10],
    messages[6],
    messages[3],
    messages[7],
    duplicates[2],
    messages[11],
  ];
}

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const modulePath = resolve(repoRoot, ".build", "src", "index.js");
const mod = await import(pathToFileURL(modulePath).href);
const { WebSocketJsonTransport, createEventId, normalizeTransportEventMessage } = mod;

const port = await getAvailablePort();
const transport = new WebSocketJsonTransport({
  mode: "server",
  host: "127.0.0.1",
  port,
  normalizeMessage: normalizeTransportEventMessage,
});

const normalizedEvents = [];
const acceptedEvents = [];
const peerStates = [];
const transportErrors = [];
const dedupe = new DedupeGateway({
  slidingWindowSeconds: 180,
  maxSlidingWindowSeconds: 300,
  nowProvider: () => BigInt(Date.now()),
});

transport.onEvent((event) => {
  normalizedEvents.push(event);
  if (dedupe.filter(event)) {
    acceptedEvents.push(event);
  }
});

transport.onPeerState((state) => {
  peerStates.push(state);
});

transport.onError((error) => {
  transportErrors.push(error);
});

const clients = [];

try {
  await transport.start();

  for (const nodeId of NODE_IDS) {
    const client = new WebSocket(`ws://127.0.0.1:${port}`);
    client.nodeId = nodeId;
    clients.push(client);
  }

  await Promise.all(clients.map((client) => once(client, "open")));

  const uniqueMessages = buildUniqueMessages();
  const allMessages = withDuplicates(uniqueMessages);
  const expectedUniqueIds = new Set(
    uniqueMessages.map((message) => createEventId(message.nodeId, message.sequence)),
  );

  for (const message of allMessages) {
    const client = clients[NODE_IDS.indexOf(message.nodeId)];
    client.send(
      JSON.stringify({
        node: message.nodeId,
        seq: String(message.sequence),
        ts: String(message.ts),
        body: message.payload,
      }),
    );
  }

  await waitFor(
    () => normalizedEvents.length === allMessages.length,
    TEST_TIMEOUT_MS,
    "all transport messages to be normalized",
  );

  assert.equal(transportErrors.length, 0, "transport should not emit errors");
  assert.equal(
    peerStates.filter((state) => state.status === "connected").length,
    NODE_IDS.length,
    "expected one connected state per client",
  );
  assert.equal(normalizedEvents.length, allMessages.length, "transport should receive every send");

  const dedupeStats = dedupe.getStats();
  assert.equal(acceptedEvents.length, uniqueMessages.length, "dedupe should keep only unique events");
  assert.equal(dedupeStats.droppedDuplicates, 3, "dedupe should drop the replayed duplicates");

  const orderResult = orderEvents(acceptedEvents, {
    detectAnomalies: true,
    allowUnknownOrder: true,
  });

  assert.equal(orderResult.ordered.length, uniqueMessages.length, "every deduped event should order");
  assert.equal(
    orderResult.anomalies.filter((anomaly) => anomaly.type === "duplicate_event").length,
    0,
    "duplicates should be removed before ordering",
  );

  const orderedIds = new Set(orderResult.ordered.map(({ event }) => event.id));
  assert.deepEqual(orderedIds, expectedUniqueIds, "ordered output should contain the expected unique events");

  for (const nodeId of NODE_IDS) {
    const nodeSequences = orderResult.ordered
      .filter(({ event }) => event.nodeId === nodeId)
      .map(({ event }) => Number(event.sequence));
    assert.deepEqual(
      nodeSequences,
      [1, 2, 3, 4],
      `ordered output should preserve same-node sequence progression for ${nodeId}`,
    );
  }

  process.stdout.write(
    `E2E pipeline test passed for transport -> dedupe -> causal-order (${acceptedEvents.length} unique events)\n`,
  );
} finally {
  dedupe.destroy();
  await Promise.allSettled(
    clients.map(
      (client) =>
        new Promise((resolveClose) => {
          if (
            client.readyState === WebSocket.CLOSING ||
            client.readyState === WebSocket.CLOSED
          ) {
            resolveClose(undefined);
            return;
          }
          client.once("close", () => resolveClose(undefined));
          client.close();
        }),
    ),
  );
  await transport.stop();
}
