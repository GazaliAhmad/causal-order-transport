# @causal-order/transport

WebSocket + JSON transport layer for normalizing node traffic into the event contract expected by the `causal-order` stack.

This package is the deployable WebSocket + JSON transport layer for the current `causal-order` stack.

## Scope

Supported ingress path:

- WebSocket + JSON

Other ingress shapes can be implemented separately against the same event contract and normalization boundary when needed.

## Relationship to `causal-order` and `@causal-order/dedupe`

`@causal-order/transport` sits before `@causal-order/dedupe` and `causal-order`.

It is responsible for:

- receiving wire-level node traffic
- decoding JSON messages from WebSocket connections
- normalizing those messages into event objects
- surfacing peer lifecycle and transport errors

In other words, this package hides network and wire-format differences so the upper runtime layers can work with one consistent event contract.

Conceptually:

```text
WebSocket + JSON -> @causal-order/transport -> @causal-order/dedupe -> causal-order
```

## What It Does

This package provides:

- a minimal transport contract for start, stop, send, and receive
- peer state reporting for connect, disconnect, and errors
- a concrete WebSocket + JSON adapter
- a default normalizer that converts common node message fields into a `causal-order` event envelope

## Install

```bash
npm install @causal-order/transport @causal-order/dedupe causal-order
```

Runtime requirements:

- Node.js `20+`
- ESM package usage

## Quick Start

```ts
import {
  WebSocketJsonTransport,
  normalizeTransportEventMessage,
} from "@causal-order/transport";

const transport = new WebSocketJsonTransport({
  mode: "server",
  port: 8080,
  normalizeMessage: normalizeTransportEventMessage,
});

transport.onEvent((event, context) => {
  console.log("event", context.peerId, event.id);
});

transport.onPeerState((state) => {
  console.log("peer", state.peerId, state.status);
});

await transport.start();
```

## Normalized Event Shape

The default normalizer tries to produce a runtime event with these fields:

- `id`
- `nodeId`
- `sequence`
- `clock.physicalTimeMs`
- `payload`
- optional `traceId`
- optional `ingestedAt`

That keeps the shape friendly for both `@causal-order/dedupe` and `causal-order`.

## Default Wire Message Shape

The adapter assumes JSON messages over WebSocket.

The default normalizer accepts common fields like:

```json
{
  "type": "event",
  "id": "edge-a-000000000042",
  "nodeId": "edge-a",
  "sequence": "42",
  "clock": {
    "physicalTimeMs": "1781000000000"
  },
  "payload": {
    "temperature": 21
  }
}
```

It also tolerates practical aliases such as:

- `node`
- `seq`
- `ts`
- `body`

## Library API

```ts
import {
  WebSocketJsonTransport,
  normalizeTransportEventMessage,
  createEventId,
} from "@causal-order/transport";
```

The intended public API is transport-first:

- `WebSocketJsonTransport`
- `normalizeTransportEventMessage()`
- `createEventId()`
- transport and peer-state types

## Validation

Validation details, including completed `2h`, `4h`, and `8h` wall-clock runs, are documented separately in [`VALIDATION.md`](https://github.com/GazaliAhmad/causal-order-transport/blob/main/VALIDATION.md).

## Conduct

Project conduct expectations are documented in [`CODE_OF_CONDUCT.md`](https://github.com/GazaliAhmad/causal-order-transport/blob/main/CODE_OF_CONDUCT.md).

## Notes

- This is the transport layer, not the dedupe layer and not the ordering core.
- This is an ESM-only package.
- The current package surface is intentionally centered on the WebSocket + JSON path.
- If another deployment needs Kafka, a broker bridge, or another wire protocol, that adapter can be built separately rather than forcing every transport mode into this package immediately.
- The normalizer is opinionated but replaceable. If your node wire format differs, provide your own `normalizeMessage` function.
