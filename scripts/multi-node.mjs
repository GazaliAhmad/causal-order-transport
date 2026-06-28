import assert from "node:assert/strict";
import net from "node:net";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import WebSocket from "ws";

const NODE_COUNT = 12;
const MESSAGES_PER_NODE = 100;
const TEST_TIMEOUT_MS = 10_000;
const MAX_WALL_CLOCK_MS = 10_000;
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
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`Timed out waiting for ${label}`);
    }
    await delay(25);
  }
}

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const modulePath = resolve(repoRoot, ".build", "src", "index.js");
const mod = await import(pathToFileURL(modulePath).href);

const { WebSocketJsonTransport, normalizeTransportEventMessage } = mod;

const port = await getAvailablePort();
const server = new WebSocketJsonTransport({
  mode: "server",
  host: "127.0.0.1",
  port,
  normalizeMessage: normalizeTransportEventMessage,
});

const receivedEvents = [];
const peerStates = [];
const transportErrors = [];

server.onEvent((event, context) => {
  receivedEvents.push({ event, context });
});

server.onPeerState((state) => {
  peerStates.push(state);
});

server.onError((error) => {
  transportErrors.push(error);
});

const clients = [];

try {
  await server.start();
  const startedAt = Date.now();

  for (let index = 0; index < NODE_COUNT; index += 1) {
    const client = new WebSocket(`ws://127.0.0.1:${port}`);
    clients.push(client);
  }

  await Promise.all(clients.map((client) => once(client, "open")));

  for (let nodeIndex = 0; nodeIndex < NODE_COUNT; nodeIndex += 1) {
    const nodeId = `node-${nodeIndex + 1}`;
    const client = clients[nodeIndex];

    for (let sequence = 1; sequence <= MESSAGES_PER_NODE; sequence += 1) {
      client.send(
        JSON.stringify({
          nodeId,
          sequence,
          clock: {
            physicalTimeMs: BASE_TIMESTAMP_MS + sequence,
            logicalCounter: 0,
            nodeId,
          },
          payload: {
            reading: nodeIndex * 1000 + sequence,
          },
        }),
      );
    }
  }

  await waitFor(
    () => receivedEvents.length === NODE_COUNT * MESSAGES_PER_NODE,
    TEST_TIMEOUT_MS,
    "all node messages to arrive",
  );

  assert.equal(transportErrors.length, 0, "transport should not emit errors");

  const connectedStates = peerStates.filter((state) => state.status === "connected");
  assert.equal(
    connectedStates.length,
    NODE_COUNT,
    `expected ${NODE_COUNT} connected peer states`,
  );

  const ids = new Set(receivedEvents.map(({ event }) => event.id));
  assert.equal(ids.size, NODE_COUNT * MESSAGES_PER_NODE, "event ids should be unique");

  for (let nodeIndex = 0; nodeIndex < NODE_COUNT; nodeIndex += 1) {
    const nodeId = `node-${nodeIndex + 1}`;
    const nodeEvents = receivedEvents.filter(({ event }) => event.nodeId === nodeId);
    assert.equal(
      nodeEvents.length,
      MESSAGES_PER_NODE,
      `expected ${MESSAGES_PER_NODE} events for ${nodeId}`,
    );

    const sequences = new Set(nodeEvents.map(({ event }) => String(event.sequence)));
    for (let sequence = 1; sequence <= MESSAGES_PER_NODE; sequence += 1) {
      assert.ok(sequences.has(String(BigInt(sequence))), `missing ${nodeId} sequence ${sequence}`);
    }
  }

  const durationMs = Date.now() - startedAt;
  assert.ok(
    durationMs <= MAX_WALL_CLOCK_MS,
    `multi-node test exceeded wall-clock budget: ${durationMs}ms`,
  );

  process.stdout.write(
    `Multi-node test passed for ${NODE_COUNT} nodes x ${MESSAGES_PER_NODE} messages in ${durationMs}ms\n`,
  );
} finally {
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
  await server.stop();
}
