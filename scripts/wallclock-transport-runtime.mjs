import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { randomUUID } from "node:crypto";
import net from "node:net";
import { once } from "node:events";

import { createHlcClock, orderEventStream } from "causal-order";
import { DedupeGateway } from "@causal-order/dedupe";
import WebSocket from "ws";

const DEFAULT_NODE_IDS = [
  "edge-a",
  "edge-b",
  "edge-c",
  "edge-d",
  "edge-e",
  "edge-f",
  "edge-g",
  "edge-h",
];

const DEFAULTS = {
  durationMs: parseDuration("2h"),
  steadyRatio: 0.3,
  reportEveryMs: parseDuration("5m"),
  maxLateArrivalMs: 60_000,
  maxDrainMs: parseDuration("2m"),
  batchSize: 200,
  outputDir: "artifacts/transport-runs",
  profile: "typical-real-world-mesh",
  runName: null,
  nodeIds: DEFAULT_NODE_IDS,
};

class AsyncQueue {
  #items = [];
  #waiters = [];
  #closed = false;

  push(value) {
    if (this.#closed) {
      throw new Error("Cannot push to a closed queue");
    }

    const waiter = this.#waiters.shift();
    if (waiter) {
      waiter({ value, done: false });
      return;
    }

    this.#items.push(value);
  }

  close() {
    if (this.#closed) {
      return;
    }

    this.#closed = true;
    while (this.#waiters.length > 0) {
      const waiter = this.#waiters.shift();
      waiter?.({ value: undefined, done: true });
    }
  }

  get size() {
    return this.#items.length;
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  next() {
    if (this.#items.length > 0) {
      return Promise.resolve({
        value: this.#items.shift(),
        done: false,
      });
    }

    if (this.#closed) {
      return Promise.resolve({
        value: undefined,
        done: true,
      });
    }

    return new Promise((resolveNext) => {
      this.#waiters.push(resolveNext);
    });
  }
}

function parseDuration(input) {
  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/i.exec(String(input).trim());
  if (!match) {
    throw new Error(`Invalid duration "${input}"`);
  }

  const value = Number(match[1]);
  const unit = match[2].toLowerCase();
  const factors = {
    ms: 1,
    s: 1_000,
    m: 60_000,
    h: 3_600_000,
  };

  return Math.floor(value * factors[unit]);
}

function formatDuration(milliseconds) {
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) {
    return "0s";
  }

  if (milliseconds < 1_000) {
    return `${Math.floor(milliseconds)}ms`;
  }

  const totalSeconds = Math.floor(milliseconds / 1_000);
  const hours = Math.floor(totalSeconds / 3_600);
  const minutes = Math.floor((totalSeconds % 3_600) / 60);
  const seconds = totalSeconds % 60;

  if (hours > 0) {
    return `${hours}h${String(minutes).padStart(2, "0")}m${String(seconds).padStart(2, "0")}s`;
  }
  if (minutes > 0) {
    return `${minutes}m${String(seconds).padStart(2, "0")}s`;
  }

  return `${seconds}s`;
}

function parseArgs(argv) {
  const config = {
    durationMs: DEFAULTS.durationMs,
    steadyForMs: null,
    steadyRatio: DEFAULTS.steadyRatio,
    reportEveryMs: DEFAULTS.reportEveryMs,
    maxLateArrivalMs: DEFAULTS.maxLateArrivalMs,
    maxDrainMs: DEFAULTS.maxDrainMs,
    batchSize: DEFAULTS.batchSize,
    outputDir: DEFAULTS.outputDir,
    profile: DEFAULTS.profile,
    runName: DEFAULTS.runName,
    nodeIds: [...DEFAULTS.nodeIds],
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    const [key, inlineValue] = token.split("=", 2);
    const value = inlineValue ?? argv[index + 1];

    switch (key) {
      case "--duration":
        config.durationMs = parseDuration(requireValue(key, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--steady-for":
        config.steadyForMs = parseDuration(requireValue(key, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--steady-ratio":
        config.steadyRatio = Number(requireValue(key, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--report-every":
        config.reportEveryMs = parseDuration(requireValue(key, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--max-late-arrival-ms":
        config.maxLateArrivalMs = Number(requireValue(key, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--max-drain":
        config.maxDrainMs = parseDuration(requireValue(key, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--batch-size":
        config.batchSize = Number(requireValue(key, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--output-dir":
        config.outputDir = requireValue(key, value);
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--profile":
        config.profile = requireValue(key, value);
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--run-name":
        config.runName = requireValue(key, value);
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--node-ids":
        config.nodeIds = requireValue(key, value)
          .split(",")
          .map((nodeId) => nodeId.trim())
          .filter(Boolean);
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--help":
        printHelp();
        process.exit(0);
        break;
      default:
        throw new Error(`Unknown argument "${token}"`);
    }
  }

  if (!Number.isFinite(config.steadyRatio) || config.steadyRatio < 0 || config.steadyRatio > 1) {
    throw new Error("--steady-ratio must be between 0 and 1");
  }
  if (!Number.isFinite(config.maxLateArrivalMs) || config.maxLateArrivalMs < 0) {
    throw new Error("--max-late-arrival-ms must be a non-negative number");
  }
  if (!Number.isFinite(config.batchSize) || config.batchSize <= 0) {
    throw new Error("--batch-size must be a positive number");
  }
  if (config.nodeIds.length === 0) {
    throw new Error("--node-ids must include at least one node");
  }

  const steadyForMs =
    config.steadyForMs ?? Math.floor(config.durationMs * config.steadyRatio);

  if (steadyForMs < 0 || steadyForMs > config.durationMs) {
    throw new Error("Steady phase must be between 0 and total duration");
  }

  return {
    ...config,
    steadyForMs,
  };
}

function printHelp() {
  process.stdout.write(
    [
      "Usage:",
      "  node scripts/wallclock-transport-runtime.mjs [options]",
      "",
      "Options:",
      "  --duration <value>           Wall-clock duration. Supports ms, s, m, h. Default: 2h",
      "  --steady-for <value>         Wall-clock steady phase before chaos",
      "  --steady-ratio <0..1>        Portion of run spent steady when --steady-for is omitted",
      "  --report-every <value>       Heartbeat interval. Default: 5m",
      "  --max-late-arrival-ms <n>    Streaming late-arrival horizon. Default: 60000",
      "  --max-drain <value>          Extra drain time after generation stops. Default: 2m",
      "  --batch-size <n>             orderEventStream batch size. Default: 200",
      "  --node-ids <csv>             Node IDs. Default: 8 edge nodes",
      "  --profile <value>            Workload profile from @causal-order/testing. Default: typical-real-world-mesh",
      "  --output-dir <path>          Artifact root. Default: artifacts/transport-runs",
      "  --run-name <value>           Optional label for the run folder",
      "  --help                       Show this message",
      "",
      "Examples:",
      "  node scripts/wallclock-transport-runtime.mjs --duration 2h --run-name typical-8node-2h",
      "  node scripts/wallclock-transport-runtime.mjs --duration 8h --report-every 10m",
    ].join("\n") + "\n",
  );
}

function requireValue(key, value) {
  if (value === undefined) {
    throw new Error(`Missing value for ${key}`);
  }

  return value;
}

function loadProfile(profileName, nodeIds) {
  const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const profilePath = resolve(
    repoRoot,
    "node_modules",
    "@causal-order",
    "testing",
    "profiles",
    `${profileName}.json`,
  );
  const profile = JSON.parse(readFileSync(profilePath, "utf8"));
  const nodeWeights = { ...profile.nodeWeights };

  for (const nodeId of nodeIds) {
    if (!(nodeId in nodeWeights)) {
      nodeWeights[nodeId] = 1;
    }
  }

  return {
    ...profile,
    nodeWeights,
  };
}

function countInto(target, key) {
  target[key] = (target[key] ?? 0) + 1;
}

function randomBetween(min, max) {
  return min + Math.random() * (max - min);
}

function sampleIntervalMs(ratePerSecond) {
  const clamped = Math.max(ratePerSecond, 0.0001);
  return Math.max(1, Math.round((-Math.log(1 - Math.random()) / clamped) * 1_000));
}

function chooseFrom(items) {
  return items[Math.floor(Math.random() * items.length)];
}

function createRunLabel(runName) {
  const timestamp = new Date().toISOString().replace(/[:]/g, "-").replace(/\..+$/, "");
  return runName ? `${timestamp}-${runName}` : timestamp;
}

function getAvailableNodeRate(nodeIds, nodeWeights, nodeId) {
  const totalWeight = nodeIds.reduce(
    (sum, currentNodeId) => sum + (nodeWeights[currentNodeId] ?? 1),
    0,
  );

  if (!Number.isFinite(totalWeight) || totalWeight <= 0) {
    return 1 / nodeIds.length;
  }

  return (nodeWeights[nodeId] ?? 1) / totalWeight;
}

function createDistribution() {
  return {
    count: 0,
    minMs: null,
    maxMs: null,
    avgMs: 0,
  };
}

function recordDistribution(distribution, value) {
  if (!Number.isFinite(value) || value < 0) {
    return;
  }

  distribution.count += 1;
  distribution.minMs = distribution.minMs === null ? value : Math.min(distribution.minMs, value);
  distribution.maxMs = distribution.maxMs === null ? value : Math.max(distribution.maxMs, value);
  distribution.avgMs = Number(
    (
      ((distribution.avgMs * (distribution.count - 1)) + value) /
      distribution.count
    ).toFixed(3),
  );
}

function createSummary(config, profile, artifacts, wallStartedAtMs) {
  return {
    outcome: {
      status: "running",
      failure: null,
    },
    config: {
      durationMs: config.durationMs,
      steadyForMs: config.steadyForMs,
      reportEveryMs: config.reportEveryMs,
      maxLateArrivalMs: config.maxLateArrivalMs,
      maxDrainMs: config.maxDrainMs,
      batchSize: config.batchSize,
      outputDir: config.outputDir,
      nodeIds: config.nodeIds,
      profile: profile.name,
      profileDescription: profile.description,
      runName: config.runName,
    },
    artifacts,
    timing: {
      wallStartedAtIso: new Date(wallStartedAtMs).toISOString(),
      wallEndedAtIso: null,
      wallElapsedMs: 0,
      generationStoppedAtIso: null,
      drainCompletedAtIso: null,
    },
    process: {
      rssBytes: {
        samples: 0,
        maxRssBytes: 0,
        finalRssBytes: 0,
        avgRssBytes: 0,
      },
      heapUsedBytes: {
        samples: 0,
        maxHeapUsedBytes: 0,
        finalHeapUsedBytes: 0,
        avgHeapUsedBytes: 0,
      },
      heapTotalBytes: {
        samples: 0,
        maxHeapTotalBytes: 0,
        finalHeapTotalBytes: 0,
        avgHeapTotalBytes: 0,
      },
      externalBytes: {
        samples: 0,
        maxExternalBytes: 0,
        finalExternalBytes: 0,
        avgExternalBytes: 0,
      },
      arrayBuffersBytes: {
        samples: 0,
        maxArrayBuffersBytes: 0,
        finalArrayBuffersBytes: 0,
        avgArrayBuffersBytes: 0,
      },
    },
    transport: {
      receivedEvents: 0,
      peerHints: 0,
      errors: 0,
      peerStatesByStatus: {},
      normalizedByNode: {},
      maxEventQueueDepth: 0,
      maxPendingDeliveries: 0,
      latencyMs: createDistribution(),
    },
    generator: {
      generatedEvents: 0,
      sentEvents: 0,
      duplicatesInjected: 0,
      sendErrors: 0,
      activeClients: 0,
      perNode: {},
    },
    dedupe: {
      acceptedEvents: 0,
      droppedDuplicates: 0,
      filterLatencyMs: createDistribution(),
    },
    ordering: {
      batches: 0,
      correctionBatches: 0,
      finalBatches: 0,
      orderedEvents: 0,
      anomalies: 0,
      byAnomalyType: {},
      byAnomalySeverity: {},
      byOrderBasis: {},
      byConfidence: {},
      maxWatermarkMs: null,
      endToEndLatencyMs: createDistribution(),
      postNormalizeLatencyMs: createDistribution(),
    },
  };
}

function makeNodeStats() {
  return {
    generatedEvents: 0,
    sentEvents: 0,
    duplicatesInjected: 0,
    sameNodeDependencies: 0,
    crossNodeDependencies: 0,
    maxPendingDeliveries: 0,
    lastScheduledSendAtMs: 0,
  };
}

function serializeHeartbeat(summary, wallNowMs, eventQueueSize, pendingDeliveries, dedupeStats) {
  const memoryUsage = process.memoryUsage();
  const rssBytes = memoryUsage.rss;

  updateMemoryMetric(summary.process.rssBytes, rssBytes, {
    max: "maxRssBytes",
    final: "finalRssBytes",
    avg: "avgRssBytes",
  });
  updateMemoryMetric(summary.process.heapUsedBytes, memoryUsage.heapUsed, {
    max: "maxHeapUsedBytes",
    final: "finalHeapUsedBytes",
    avg: "avgHeapUsedBytes",
  });
  updateMemoryMetric(summary.process.heapTotalBytes, memoryUsage.heapTotal, {
    max: "maxHeapTotalBytes",
    final: "finalHeapTotalBytes",
    avg: "avgHeapTotalBytes",
  });
  updateMemoryMetric(summary.process.externalBytes, memoryUsage.external, {
    max: "maxExternalBytes",
    final: "finalExternalBytes",
    avg: "avgExternalBytes",
  });
  updateMemoryMetric(summary.process.arrayBuffersBytes, memoryUsage.arrayBuffers, {
    max: "maxArrayBuffersBytes",
    final: "finalArrayBuffersBytes",
    avg: "avgArrayBuffersBytes",
  });

  return {
    timestampIso: new Date(wallNowMs).toISOString(),
    elapsedMs: wallNowMs - Date.parse(summary.timing.wallStartedAtIso),
    transport: {
      receivedEvents: summary.transport.receivedEvents,
      maxEventQueueDepth: Math.max(summary.transport.maxEventQueueDepth, eventQueueSize),
      maxPendingDeliveries: Math.max(summary.transport.maxPendingDeliveries, pendingDeliveries),
    },
    generator: {
      generatedEvents: summary.generator.generatedEvents,
      sentEvents: summary.generator.sentEvents,
      duplicatesInjected: summary.generator.duplicatesInjected,
    },
    dedupe: {
      acceptedEvents: summary.dedupe.acceptedEvents,
      droppedDuplicates: summary.dedupe.droppedDuplicates,
      cacheSize: dedupeStats.currentCacheSize,
      activeWindowSeconds: dedupeStats.activeWindowSeconds,
    },
    ordering: {
      orderedEvents: summary.ordering.orderedEvents,
      anomalies: summary.ordering.anomalies,
      batches: summary.ordering.batches,
    },
    process: {
      rssBytes,
      heapUsedBytes: memoryUsage.heapUsed,
      heapTotalBytes: memoryUsage.heapTotal,
      externalBytes: memoryUsage.external,
      arrayBuffersBytes: memoryUsage.arrayBuffers,
    },
  };
}

function updateMemoryMetric(bucket, value, keys) {
  bucket.samples += 1;
  bucket[keys.max] = Math.max(bucket[keys.max], value);
  bucket[keys.final] = value;
  const previousAverage = bucket[keys.avg];
  const sampleCount = bucket.samples;
  bucket[keys.avg] = Math.round(
    ((previousAverage * (sampleCount - 1)) + value) / sampleCount,
  );
}

function samplePhase(profile, wallNowMs, wallStartedAtMs, steadyForMs) {
  return wallNowMs - wallStartedAtMs < steadyForMs ? "steady" : "chaotic";
}

function sampleDeliveryDelayMs(profile, phase) {
  const delays = phase === "steady" ? profile.delays.steady : profile.delays.chaotic;
  let delayMs = Math.round(randomBetween(delays.baseMinMs, delays.baseMaxMs));

  if (phase === "steady") {
    if (Math.random() < delays.spikeChance) {
      delayMs += Math.round(randomBetween(delays.spikeMinMs, delays.spikeMaxMs));
    }
    return delayMs;
  }

  if (Math.random() < delays.slowSpikeChance) {
    delayMs += Math.round(randomBetween(delays.slowSpikeMinMs, delays.slowSpikeMaxMs));
  }
  if (Math.random() < delays.lateSpikeChance) {
    delayMs += Math.round(randomBetween(delays.lateSpikeMinMs, delays.lateSpikeMaxMs));
  }
  if (Math.random() < delays.extremeSpikeChance) {
    delayMs += Math.round(randomBetween(delays.extremeSpikeMinMs, delays.extremeSpikeMaxMs));
  }

  return delayMs;
}

function chooseDependency(profile, phase, nodeState, recentGlobal) {
  const sameNodeChance =
    phase === "steady"
      ? profile.dependencies.steadySameNodeChance
      : profile.dependencies.chaoticSameNodeChance;
  const crossNodeChance =
    phase === "steady"
      ? profile.dependencies.steadyCrossNodeChance
      : profile.dependencies.chaoticCrossNodeChance;
  const roll = Math.random();

  if (roll < crossNodeChance) {
    const remoteCandidates = recentGlobal.filter((event) => event.nodeId !== nodeState.nodeId);
    if (remoteCandidates.length > 0) {
      return {
        event: chooseFrom(remoteCandidates),
        relation: "cross-node",
      };
    }
  }

  if (roll < crossNodeChance + sameNodeChance && nodeState.recentLocal.length > 0) {
    return {
      event: chooseFrom(nodeState.recentLocal),
      relation: "same-node",
    };
  }

  return null;
}

function rememberRecent(target, event, limit) {
  target.unshift(event);
  if (target.length > limit) {
    target.length = limit;
  }
}

function chooseOperation(phase, relation) {
  if (relation === "cross-node") {
    return chooseFrom([
      "replica.applied",
      "projection.updated",
      "payment.confirmed",
      "inventory.reserved",
      "workflow.forwarded",
    ]);
  }

  if (phase === "steady") {
    return chooseFrom([
      "ingress.accepted",
      "order.created",
      "state.persisted",
      "workflow.advanced",
    ]);
  }

  return chooseFrom([
    "retry.dispatched",
    "reconciliation.applied",
    "projection.rebuilt",
    "workflow.recovered",
  ]);
}

function createEventRecord(nodeState, profile, phase, recentGlobal, createEventId) {
  const dependency = chooseDependency(profile, phase, nodeState, recentGlobal);
  let clock;

  if (dependency?.relation === "cross-node") {
    clock = nodeState.hlc.receive(dependency.event.clock);
    nodeState.stats.crossNodeDependencies += 1;
  } else {
    clock = nodeState.hlc.now();
    if (dependency?.relation === "same-node") {
      nodeState.stats.sameNodeDependencies += 1;
    }
  }

  nodeState.sequence += 1n;
  nodeState.stats.generatedEvents += 1;

  const traceId = dependency?.event.traceId ?? randomUUID();
  const entityId = dependency?.event.entityId ?? `entity-${nodeState.nodeId}-${nodeState.sequence}`;
  const sentAtMs = Date.now();
  const eventId = createEventId(nodeState.nodeId, nodeState.sequence);

  const recentEvent = {
    id: eventId,
    nodeId: nodeState.nodeId,
    clock,
    traceId,
    entityId,
  };

  const payload = {
    phase,
    service: nodeState.nodeId,
    entityId,
    traceId,
    operation: chooseOperation(phase, dependency?.relation ?? null),
    sentAtMs,
    dependencyKind: dependency?.relation ?? null,
  };

  const message = {
    type: "event",
    id: eventId,
    nodeId: nodeState.nodeId,
    sequence: String(nodeState.sequence),
    clock: {
      physicalTimeMs: String(clock.physicalTimeMs),
      logicalCounter: clock.logicalCounter,
      nodeId: clock.nodeId,
    },
    payload,
    traceId,
  };

  return {
    message,
    recentEvent,
  };
}

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

async function waitForDrain(predicate, timeoutMs, label) {
  const startedAt = Date.now();
  while (!predicate()) {
    if (Date.now() - startedAt > timeoutMs) {
      throw new Error(`Timed out waiting for ${label}`);
    }
    await delay(100);
  }
}

const config = parseArgs(process.argv.slice(2));
const profile = loadProfile(config.profile, config.nodeIds);

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const modulePath = resolve(repoRoot, ".build", "src", "index.js");
const transportModule = await import(pathToFileURL(modulePath).href);
const {
  WebSocketJsonTransport,
  createEventId,
  normalizeTransportEventMessage,
} = transportModule;

const wallStartedAtMs = Date.now();
const runLabel = createRunLabel(config.runName);
const runDir = resolve(config.outputDir, runLabel);
const artifacts = {
  runDir,
  summaryPath: resolve(runDir, "summary.json"),
  heartbeatPath: resolve(runDir, "heartbeat.ndjson"),
  anomaliesPath: resolve(runDir, "anomalies.ndjson"),
  configPath: resolve(runDir, "config.json"),
};

mkdirSync(runDir, { recursive: true });
writeFileSync(
  artifacts.configPath,
  `${JSON.stringify(
    {
      ...config,
      profile: {
        name: profile.name,
        description: profile.description,
        nodeWeights: profile.nodeWeights,
      },
    },
    null,
    2,
  )}\n`,
  "utf8",
);

const summary = createSummary(config, profile, artifacts, wallStartedAtMs);
const eventQueue = new AsyncQueue();
const port = await getAvailablePort();
const transport = new WebSocketJsonTransport({
  mode: "server",
  host: "127.0.0.1",
  port,
  normalizeMessage: normalizeTransportEventMessage,
});
const dedupe = new DedupeGateway({
  preset: "standard",
  nowProvider: () => BigInt(Date.now()),
});

const recentGlobal = [];
const scheduledTimeouts = new Set();
const clients = [];
const nodeStates = new Map();
let pendingDeliveries = 0;
let stopRequested = false;
let heartbeatTimer = null;

for (const nodeId of config.nodeIds) {
  nodeStates.set(nodeId, {
    nodeId,
    sequence: 0n,
    hlc: createHlcClock({
      nodeId,
      now: () => BigInt(Date.now()),
    }),
    recentLocal: [],
    stats: makeNodeStats(),
  });
  summary.generator.perNode[nodeId] = nodeStates.get(nodeId).stats;
}

function log(message) {
  process.stdout.write(`${new Date().toISOString()} ${message}\n`);
}

function appendHeartbeat() {
  const dedupeStats = dedupe.getStats();
  const heartbeat = serializeHeartbeat(
    summary,
    Date.now(),
    eventQueue.size,
    pendingDeliveries,
    dedupeStats,
  );
  appendFileSync(artifacts.heartbeatPath, `${JSON.stringify(heartbeat)}\n`, "utf8");
}

transport.onEvent((event, context) => {
  const sentAtMs = Number(event.payload?.sentAtMs);
  const receivedAtMs = Number(context.receivedAtMs);
  const transportLatencyMs = receivedAtMs - sentAtMs;

  summary.transport.receivedEvents += 1;
  countInto(summary.transport.normalizedByNode, event.nodeId);
  recordDistribution(summary.transport.latencyMs, transportLatencyMs);
  summary.transport.maxEventQueueDepth = Math.max(summary.transport.maxEventQueueDepth, eventQueue.size);

  const filterStartedAt = process.hrtime.bigint();
  const enrichedEvent = {
    ...event,
    ingestedAt: context.receivedAtMs,
  };
  const accepted = dedupe.filter(enrichedEvent);
  const filterElapsedMs = Number(process.hrtime.bigint() - filterStartedAt) / 1_000_000;
  recordDistribution(summary.dedupe.filterLatencyMs, filterElapsedMs);

  if (!accepted) {
    summary.dedupe.droppedDuplicates += 1;
    return;
  }

  summary.dedupe.acceptedEvents += 1;
  eventQueue.push(enrichedEvent);
  summary.transport.maxEventQueueDepth = Math.max(summary.transport.maxEventQueueDepth, eventQueue.size);
});

transport.onPeerState((state) => {
  countInto(summary.transport.peerStatesByStatus, state.status);
});

transport.onError((error) => {
  summary.transport.errors += 1;
  appendFileSync(
    artifacts.anomaliesPath,
    `${JSON.stringify({
      timestampIso: new Date().toISOString(),
      kind: "transport_error",
      detail: error.detail,
      peerId: error.peerId ?? null,
      connectionId: error.connectionId ?? null,
    })}\n`,
    "utf8",
  );
});

const orderingPromise = (async () => {
  for await (const batch of orderEventStream(eventQueue, {
    batchSize: config.batchSize,
    maxLateArrivalMs: BigInt(config.maxLateArrivalMs),
    lateArrivalPolicy: "flag",
    allowUnknownOrder: true,
    detectAnomalies: true,
  })) {
    summary.ordering.batches += 1;
    summary.ordering.orderedEvents += batch.events.length;
    summary.ordering.anomalies += batch.anomalies.length;
    if (batch.correction) {
      summary.ordering.correctionBatches += 1;
    }
    if (batch.isFinal) {
      summary.ordering.finalBatches += 1;
    }
    if (
      summary.ordering.maxWatermarkMs === null ||
      batch.watermark > BigInt(summary.ordering.maxWatermarkMs)
    ) {
      summary.ordering.maxWatermarkMs = batch.watermark.toString();
    }

    const batchObservedAtMs = Date.now();
    for (const orderedEvent of batch.events) {
      countInto(summary.ordering.byOrderBasis, orderedEvent.orderBasis);
      countInto(summary.ordering.byConfidence, orderedEvent.confidence);

      const sentAtMs = Number(orderedEvent.event.payload?.sentAtMs);
      const ingestedAtMs = Number(orderedEvent.event.ingestedAt ?? 0n);
      recordDistribution(summary.ordering.endToEndLatencyMs, batchObservedAtMs - sentAtMs);
      recordDistribution(summary.ordering.postNormalizeLatencyMs, batchObservedAtMs - ingestedAtMs);
    }

    for (const anomaly of batch.anomalies) {
      countInto(summary.ordering.byAnomalyType, anomaly.type);
      countInto(summary.ordering.byAnomalySeverity, anomaly.severity);
      appendFileSync(
        artifacts.anomaliesPath,
        `${JSON.stringify({
          timestampIso: new Date().toISOString(),
          kind: "ordering_anomaly",
          type: anomaly.type,
          severity: anomaly.severity,
          eventId: anomaly.event?.id ?? null,
          nodeId: anomaly.event?.nodeId ?? null,
          relatedEventIds: anomaly.relatedEvents?.map((event) => event.id) ?? [],
          message: anomaly.message,
        })}\n`,
        "utf8",
      );
    }
  }
})();

const wallEndedAtMs = wallStartedAtMs + config.durationMs;

function scheduleSend(nodeState, message, sendAtMs, isDuplicate) {
  pendingDeliveries += 1;
  summary.transport.maxPendingDeliveries = Math.max(summary.transport.maxPendingDeliveries, pendingDeliveries);
  nodeState.stats.maxPendingDeliveries = Math.max(
    nodeState.stats.maxPendingDeliveries,
    pendingDeliveries,
  );

  const timeout = setTimeout(() => {
    scheduledTimeouts.delete(timeout);
    try {
      nodeState.client.send(JSON.stringify(message));
      nodeState.stats.sentEvents += 1;
      summary.generator.sentEvents += 1;
      if (isDuplicate) {
        nodeState.stats.duplicatesInjected += 1;
        summary.generator.duplicatesInjected += 1;
      }
    } catch (error) {
      summary.generator.sendErrors += 1;
      appendFileSync(
        artifacts.anomaliesPath,
        `${JSON.stringify({
          timestampIso: new Date().toISOString(),
          kind: "send_error",
          nodeId: nodeState.nodeId,
          detail: error instanceof Error ? error.message : String(error),
        })}\n`,
        "utf8",
      );
    } finally {
      pendingDeliveries -= 1;
    }
  }, Math.max(0, sendAtMs - Date.now()));

  scheduledTimeouts.add(timeout);
}

function buildNodeLoop(nodeState) {
  const nodeRateShare = getAvailableNodeRate(
    config.nodeIds,
    profile.nodeWeights,
    nodeState.nodeId,
  );

  return (async () => {
    while (!stopRequested && Date.now() < wallEndedAtMs) {
      const phase = samplePhase(profile, Date.now(), wallStartedAtMs, config.steadyForMs);
      const phaseRate =
        phase === "steady"
          ? profile.phaseRates.steadyEventsPerSecond
          : profile.phaseRates.steadyEventsPerSecond *
            profile.phaseRates.chaosMultiplier *
            randomBetween(profile.phaseRates.chaosJitterMin, profile.phaseRates.chaosJitterMax);

      const nodeRate = Math.max(0.001, phaseRate * nodeRateShare);
      await delay(sampleIntervalMs(nodeRate));

      if (stopRequested || Date.now() >= wallEndedAtMs) {
        break;
      }

      const { message, recentEvent } = createEventRecord(
        nodeState,
        profile,
        phase,
        recentGlobal,
        createEventId,
      );
      summary.generator.generatedEvents += 1;
      rememberRecent(nodeState.recentLocal, recentEvent, 48);
      rememberRecent(recentGlobal, recentEvent, 256);

      const baseDelayMs = sampleDeliveryDelayMs(profile, phase);
      let sendAtMs = Date.now() + baseDelayMs;
      const preserveOrderChance =
        phase === "steady"
          ? profile.ordering.steadyPreserveOrderChance
          : profile.ordering.chaoticPreserveOrderChance;

      if (Math.random() < preserveOrderChance) {
        sendAtMs = Math.max(sendAtMs, nodeState.stats.lastScheduledSendAtMs + 1);
      }

      nodeState.stats.lastScheduledSendAtMs = sendAtMs;
      scheduleSend(nodeState, message, sendAtMs, false);

      const duplicateChance =
        phase === "steady"
          ? profile.duplicates.steadyChance
          : profile.duplicates.chaoticChance;

      if (Math.random() < duplicateChance) {
        scheduleSend(
          nodeState,
          message,
          sendAtMs + Math.round(randomBetween(200, 3_000)),
          true,
        );
      }
    }
  })();
}

process.once("SIGINT", () => {
  stopRequested = true;
});
process.once("SIGTERM", () => {
  stopRequested = true;
});

try {
  await transport.start();

  for (const nodeId of config.nodeIds) {
    const client = new WebSocket(`ws://127.0.0.1:${port}`);
    const nodeState = nodeStates.get(nodeId);
    nodeState.client = client;
    clients.push(client);

    client.on("open", () => {
      summary.generator.activeClients += 1;
    });
    client.on("close", () => {
      summary.generator.activeClients = Math.max(0, summary.generator.activeClients - 1);
    });
    client.on("error", (error) => {
      summary.generator.sendErrors += 1;
      appendFileSync(
        artifacts.anomaliesPath,
        `${JSON.stringify({
          timestampIso: new Date().toISOString(),
          kind: "client_error",
          nodeId,
          detail: error.message,
        })}\n`,
        "utf8",
      );
    });
  }

  await Promise.all(clients.map((client) => once(client, "open")));
  log(
    [
      `starting wall-clock transport runtime`,
      `duration=${formatDuration(config.durationMs)}`,
      `steady=${formatDuration(config.steadyForMs)}`,
      `nodes=${config.nodeIds.length}`,
      `profile=${profile.name}`,
      `runDir=${runDir}`,
    ].join(" | "),
  );

  appendHeartbeat();
  heartbeatTimer = setInterval(() => {
    appendHeartbeat();
    const dedupeStats = dedupe.getStats();
    log(
      [
        `[wall ${formatDuration(Date.now() - wallStartedAtMs)}]`,
        `generated=${summary.generator.generatedEvents}`,
        `sent=${summary.generator.sentEvents}`,
        `received=${summary.transport.receivedEvents}`,
        `accepted=${summary.dedupe.acceptedEvents}`,
        `dropped=${summary.dedupe.droppedDuplicates}`,
        `ordered=${summary.ordering.orderedEvents}`,
        `anomalies=${summary.ordering.anomalies}`,
        `queue=${eventQueue.size}`,
        `pending=${pendingDeliveries}`,
        `dedupeCache=${dedupeStats.currentCacheSize}`,
      ].join(" "),
    );
  }, config.reportEveryMs);

  try {
    await Promise.all(
      [...nodeStates.values()].map((nodeState) => buildNodeLoop(nodeState)),
    );
  } finally {
    summary.timing.generationStoppedAtIso = new Date().toISOString();
  }

  await waitForDrain(
    () => pendingDeliveries === 0,
    config.maxDrainMs,
    "pending websocket deliveries to drain",
  );

  summary.timing.drainCompletedAtIso = new Date().toISOString();
  eventQueue.close();
  await orderingPromise;

  clearInterval(heartbeatTimer);
  appendHeartbeat();

  summary.outcome.status = stopRequested ? "interrupted" : "completed";
  } catch (error) {
    summary.outcome.status = "failed";
    summary.outcome.failure = {
      message: error instanceof Error ? error.message : String(error),
    };
    throw error;
} finally {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
  }

  for (const timeout of scheduledTimeouts) {
    clearTimeout(timeout);
  }

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

  const wallEndedAtFinalMs = Date.now();
  summary.timing.wallEndedAtIso = new Date(wallEndedAtFinalMs).toISOString();
  summary.timing.wallElapsedMs = wallEndedAtFinalMs - wallStartedAtMs;

  const dedupeStats = dedupe.getStats();
  summary.dedupe.cacheSize = dedupeStats.currentCacheSize;
  summary.dedupe.activeWindowSeconds = dedupeStats.activeWindowSeconds;

  writeFileSync(`${artifacts.summaryPath}`, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
  dedupe.destroy();
  log(
    [
      `completed status=${summary.outcome.status}`,
      `elapsed=${formatDuration(summary.timing.wallElapsedMs)}`,
      `generated=${summary.generator.generatedEvents}`,
      `received=${summary.transport.receivedEvents}`,
      `accepted=${summary.dedupe.acceptedEvents}`,
      `ordered=${summary.ordering.orderedEvents}`,
      `anomalies=${summary.ordering.anomalies}`,
      `summary=${artifacts.summaryPath}`,
    ].join(" | "),
  );
}
