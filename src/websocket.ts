import { randomUUID } from "node:crypto";

import WebSocket, { WebSocketServer } from "ws";

import type {
  NormalizedTransportEvent,
  TransportContract,
  TransportErrorHandler,
  TransportEventHandler,
  TransportPeerState,
  TransportPeerStateHandler,
  WebSocketJsonTransportOptions,
} from "./types.js";
import { normalizeTransportEventMessage } from "./normalize.js";

type PeerRecord = {
  peerId: string;
  connectionId: string;
  socket: WebSocket;
};

export class WebSocketJsonTransport<T = Record<string, unknown>>
  implements TransportContract<T>
{
  #options: WebSocketJsonTransportOptions<T>;
  #server: WebSocketServer | null;
  #client: WebSocket | null;
  #peers: Map<string, PeerRecord>;
  #eventHandlers: Set<TransportEventHandler<T>>;
  #peerHandlers: Set<TransportPeerStateHandler>;
  #errorHandlers: Set<TransportErrorHandler>;

  constructor(options: WebSocketJsonTransportOptions<T>) {
    this.#options = options;
    this.#server = null;
    this.#client = null;
    this.#peers = new Map();
    this.#eventHandlers = new Set();
    this.#peerHandlers = new Set();
    this.#errorHandlers = new Set();
  }

  async start(): Promise<void> {
    if (this.#options.mode === "server") {
      await this.#startServer();
      return;
    }

    await this.#startClient();
  }

  async stop(): Promise<void> {
    for (const peer of this.#peers.values()) {
      peer.socket.close();
    }
    this.#peers.clear();

    if (this.#client) {
      this.#client.close();
      this.#client = null;
    }

    if (this.#server) {
      await new Promise<void>((resolveStop) => {
        this.#server?.close(() => resolveStop());
      });
      this.#server = null;
    }
  }

  async send(event: NormalizedTransportEvent<T>, targetPeerId?: string): Promise<void> {
    const message = JSON.stringify({
      type: "event",
      ...event,
    });

    if (targetPeerId) {
      const peer = this.#peers.get(targetPeerId);
      if (!peer) {
        throw new Error(`Unknown peer "${targetPeerId}"`);
      }
      peer.socket.send(message);
      return;
    }

    for (const peer of this.#peers.values()) {
      peer.socket.send(message);
    }
  }

  onEvent(handler: TransportEventHandler<T>): () => void {
    this.#eventHandlers.add(handler);
    return () => this.#eventHandlers.delete(handler);
  }

  onPeerState(handler: TransportPeerStateHandler): () => void {
    this.#peerHandlers.add(handler);
    return () => this.#peerHandlers.delete(handler);
  }

  onError(handler: TransportErrorHandler): () => void {
    this.#errorHandlers.add(handler);
    return () => this.#errorHandlers.delete(handler);
  }

  async #startServer(): Promise<void> {
    if (!this.#options.port) {
      throw new Error('Server mode requires "port"');
    }

    this.#server = new WebSocketServer({
      host: this.#options.host ?? "127.0.0.1",
      port: this.#options.port,
    });

    this.#server.on("connection", (socket) => {
      const connectionId = randomUUID();
      const peerId = `peer-${connectionId}`;
      const peer = { peerId, connectionId, socket };
      this.#peers.set(peerId, peer);
      this.#emitPeerState({ peerId, connectionId, status: "connected" });
      this.#attachSocket(peer);
    });

    this.#server.on("error", (error) => {
      this.#emitError({ detail: error.message, cause: error });
    });
  }

  async #startClient(): Promise<void> {
    if (!this.#options.url) {
      throw new Error('Client mode requires "url"');
    }

    const socket = new WebSocket(this.#options.url);
    this.#client = socket;
    const connectionId = randomUUID();
    const peerId = this.#options.peerId ?? "server";

    await new Promise<void>((resolveStart, rejectStart) => {
      socket.once("open", () => {
        const peer = { peerId, connectionId, socket };
        this.#peers.set(peerId, peer);
        this.#emitPeerState({ peerId, connectionId, status: "connected" });
        this.#attachSocket(peer);
        resolveStart();
      });

      socket.once("error", (error) => {
        this.#emitError({ peerId, connectionId, detail: error.message, cause: error });
        rejectStart(error);
      });
    });
  }

  #attachSocket(peer: PeerRecord): void {
    peer.socket.on("message", (raw) => {
      try {
        const parsed = JSON.parse(String(raw));
        const normalizer = this.#options.normalizeMessage ?? normalizeTransportEventMessage;
        const event = normalizer(parsed);
        const receivedAtMs = BigInt(Date.now());
        for (const handler of this.#eventHandlers) {
          handler(event, {
            peerId: peer.peerId,
            connectionId: peer.connectionId,
            receivedAtMs,
            mode: this.#options.mode,
            rawMessage: parsed,
          });
        }
      } catch (error) {
        this.#emitError({
          peerId: peer.peerId,
          connectionId: peer.connectionId,
          detail: error instanceof Error ? error.message : String(error),
          cause: error,
        });
      }
    });

    peer.socket.on("close", () => {
      this.#peers.delete(peer.peerId);
      this.#emitPeerState({
        peerId: peer.peerId,
        connectionId: peer.connectionId,
        status: "disconnected",
      });
    });

    peer.socket.on("error", (error) => {
      this.#emitPeerState({
        peerId: peer.peerId,
        connectionId: peer.connectionId,
        status: "error",
        detail: error.message,
      });
      this.#emitError({
        peerId: peer.peerId,
        connectionId: peer.connectionId,
        detail: error.message,
        cause: error,
      });
    });
  }

  #emitPeerState(state: TransportPeerState): void {
    for (const handler of this.#peerHandlers) {
      handler(state);
    }
  }

  #emitError(error: {
    peerId?: string;
    connectionId?: string;
    detail: string;
    cause?: unknown;
  }): void {
    for (const handler of this.#errorHandlers) {
      handler(error);
    }
  }
}
