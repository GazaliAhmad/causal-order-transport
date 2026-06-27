import { resolve } from "node:path";
import { dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const modulePath = resolve(repoRoot, ".build", "src", "index.js");

const mod = await import(pathToFileURL(modulePath).href);

const event = mod.normalizeTransportEventMessage({
  node: "edge-a",
  seq: "42",
  ts: "1781000000000",
  body: {
    temperature: 21,
  },
});

if (event.id !== "edge-a-000000000042") {
  throw new Error(`Unexpected normalized event id: ${event.id}`);
}

if (event.nodeId !== "edge-a") {
  throw new Error(`Unexpected normalized nodeId: ${event.nodeId}`);
}

if (event.payload?.temperature !== 21) {
  throw new Error("Unexpected normalized payload");
}

process.stdout.write("Smoke test passed for @causal-order/transport\n");
