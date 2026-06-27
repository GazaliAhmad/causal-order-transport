import type { NormalizedTransportEvent } from "./types.js";

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Transport message must be a JSON object");
  }

  return value as Record<string, unknown>;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function asBigIntLike(value: unknown): bigint | string | number | undefined {
  if (typeof value === "bigint") {
    return value;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim().length > 0) {
    return value;
  }
  return undefined;
}

function toBigInt(value: bigint | string | number | undefined, label: string): bigint {
  if (typeof value === "bigint") {
    return value;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return BigInt(Math.trunc(value));
  }
  if (typeof value === "string" && value.trim().length > 0) {
    return BigInt(value);
  }

  throw new Error(`${label} must be a bigint-compatible value`);
}

function toNumber(value: unknown, fallback: number): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return fallback;
}

function resolveClock(nodeId: string, record: Record<string, unknown>) {
  if ("clock" in record) {
    const clock = asRecord(record.clock);
    const physicalTimeMs = asBigIntLike(clock.physicalTimeMs);
    if (physicalTimeMs !== undefined) {
      return {
        physicalTimeMs: toBigInt(physicalTimeMs, "clock.physicalTimeMs"),
        logicalCounter: toNumber(clock.logicalCounter ?? clock.logical ?? clock.lc, 0),
        nodeId: asString(clock.nodeId) ?? nodeId,
      };
    }
  }

  const timestamp = asBigIntLike(record.ts ?? record.timestampMs ?? record.physicalTimeMs);
  if (timestamp !== undefined) {
    return {
      physicalTimeMs: toBigInt(timestamp, "timestamp"),
      logicalCounter: toNumber(record.logicalCounter ?? record.logical ?? record.lc, 0),
      nodeId,
    };
  }

  throw new Error("Transport message must include clock.physicalTimeMs or a timestamp alias");
}

export function createEventId(nodeId: string, sequence: bigint | string | number): string {
  return `${nodeId}-${String(sequence).padStart(12, "0")}`;
}

export function normalizeTransportEventMessage<T = Record<string, unknown>>(
  value: unknown,
): NormalizedTransportEvent<T> {
  const record = asRecord(value);
  const nodeId = asString(record.nodeId ?? record.node ?? record.sourceNodeId);
  if (!nodeId) {
    throw new Error("Transport message must include nodeId or node");
  }

  const sequence = asBigIntLike(record.sequence ?? record.seq);
  const payload = (record.payload ?? record.body ?? {}) as T;
  const id =
    asString(record.id) ??
    (sequence !== undefined ? createEventId(nodeId, sequence) : undefined);
  if (!id) {
    throw new Error("Transport message must include id or sequence/seq");
  }

  return {
    id,
    nodeId,
    sequence: sequence !== undefined ? toBigInt(sequence, "sequence") : undefined,
    clock: resolveClock(nodeId, record),
    payload,
    traceId: asString(record.traceId),
    ingestedAt:
      record.ingestedAt !== undefined
        ? toBigInt(asBigIntLike(record.ingestedAt), "ingestedAt")
        : undefined,
  };
}
