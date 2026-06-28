# Changelog

All notable changes to `@causal-order/transport` will be documented in this file.

## 0.1.1

- Clarified the README scope so the package is explicitly positioned as the current WebSocket + JSON transport layer for the `causal-order` stack.
- Tightened the README so it reads more like an npm package reference, including public GitHub links to validation scripts.
- Added a 12-node multi-node transport test for concurrent WebSocket + JSON ingress verification.
- Added an end-to-end pipeline test covering `transport -> @causal-order/dedupe -> causal-order`.
- Added a smoke-level verification script for the published `@causal-order/testing` runtime package.
- Added a transport-aware wall-clock runtime harness for long-running `WebSocket -> transport -> dedupe -> causal-order` validation with timing, heartbeats, anomaly logs, and run summaries.
- Added wall-clock memory summary fields including `maxRssBytes`, `finalRssBytes`, and `avgRssBytes`.
- Moved `@causal-order/testing` to `devDependencies` so publish installs stay runtime-focused.
- Confirmed the published npm tarball is limited to built runtime files and package documentation.

## 0.1.0

- Introduced a standalone transport package for the `causal-order` ecosystem.
- Added a WebSocket + JSON reference adapter that normalizes incoming node traffic into canonical runtime events.
- Added a small transport contract for event delivery, peer state, and transport errors.
- Added smoke-test, CI, and npm publish scaffolding for first release readiness.
