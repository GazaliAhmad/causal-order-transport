import type { EventEnvelope } from "causal-order/types";

export type TransportEventPayload = Record<string, unknown>;

export type NormalizedTransportEvent<T = TransportEventPayload> = EventEnvelope<T> & {
  sequence?: bigint;
  traceId?: string;
  ingestedAt?: bigint;
};

export type TransportMode = "server" | "client";

export interface TransportContext {
  peerId: string;
  connectionId: string;
  receivedAtMs: bigint;
  mode: TransportMode;
  rawMessage: unknown;
}

export interface TransportPeerState {
  peerId: string;
  connectionId: string;
  status: "connecting" | "connected" | "disconnected" | "error";
  detail?: string;
}

export interface TransportError {
  peerId?: string;
  connectionId?: string;
  detail: string;
  cause?: unknown;
}

export type TransportEventHandler<T = TransportEventPayload> = (
  event: NormalizedTransportEvent<T>,
  context: TransportContext,
) => void;

export type TransportPeerStateHandler = (state: TransportPeerState) => void;

export type TransportErrorHandler = (error: TransportError) => void;

export interface TransportContract<T = TransportEventPayload> {
  start(): Promise<void>;
  stop(): Promise<void>;
  send(event: NormalizedTransportEvent<T>, targetPeerId?: string): Promise<void>;
  onEvent(handler: TransportEventHandler<T>): () => void;
  onPeerState(handler: TransportPeerStateHandler): () => void;
  onError(handler: TransportErrorHandler): () => void;
}

export interface WebSocketJsonTransportOptions<T = TransportEventPayload> {
  mode: TransportMode;
  url?: string;
  port?: number;
  host?: string;
  peerId?: string;
  normalizeMessage?: (message: unknown) => NormalizedTransportEvent<T>;
}
