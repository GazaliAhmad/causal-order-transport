# @causal-order/transport

WebSocket + JSON transport layer for normalizing node traffic into the event contract expected by the `causal-order` stack.

Status: ready for early use. This package is meant to be the first transport layer in the `causal-order` ecosystem, with WebSocket + JSON as the v0.1 transport model and room for more adapters later.

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

This package gives you:

- a minimal transport contract for start, stop, send, and receive
- peer state reporting for connect, disconnect, and errors
- a reference WebSocket + JSON adapter
- a default normalizer that converts common node message fields into a `causal-order` event envelope

## Install

From npm as a user of the package:

```bash
npm install @causal-order/transport @causal-order/dedupe causal-order
```

From source while working on the package itself:

```bash
npm install
```

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

The first adapter assumes nodes transmit JSON messages over WebSocket.

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

## For Maintainers

This package is the first concrete transport extraction for the `causal-order` ecosystem. It intentionally starts small around one real-world-common mode: JSON messages over long-lived WebSocket connections.

### Local Development

```bash
npm run build
npm run test:smoke
npm run ci
npm run pack:check
```

### First Manual npm Publish

For the first publish, keep it manual before relying on GitHub Actions:

```bash
npm login
npm run release:check
npm publish
```

### GitHub Repo Setup

If you move this into its own GitHub repository, the current package metadata assumes:

- repo: `https://github.com/GazaliAhmad/causal-order-transport`
- package: `@causal-order/transport`

Before first publish:

1. Create the GitHub repository.
2. Add an `NPM_TOKEN` repository secret with npm publish access.
3. Push the default branch and confirm the `CI` workflow passes.
4. Publish either with the `Publish` workflow or from a GitHub Release event.

## Notes

- This is the transport layer, not the dedupe layer and not the ordering core.
- The v0.1 package is designed for multiple adapters later, but intentionally implements only the WebSocket + JSON path first.
- The normalizer is opinionated but replaceable. If your node wire format differs, provide your own `normalizeMessage` function.
