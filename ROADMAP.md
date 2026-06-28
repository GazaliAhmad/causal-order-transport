# Roadmap

## Direction

`@causal-order/transport` is currently the WebSocket + JSON transport layer for the `causal-order` stack.

The near-term goal is to harden that one ingress path well before expanding into additional transport-specific adapters.

The package is intended to sit here:

```text
WebSocket + JSON -> @causal-order/transport -> @causal-order/dedupe -> causal-order
```

## Near Term

- Publish `0.1.1` with the current packaging, README, changelog, and validation improvements.
- Complete longer wall-clock validation runs:
  - `4h`
  - `8h`
  - optionally `12h`
- Confirm the long-run validation remains healthy on:
  - memory stability
  - bounded dedupe cache behavior
  - anomaly severity distribution
  - final `accepted == ordered` completion behavior
- Keep the npm package surface small and runtime-focused.

## Stabilization

- Treat the WebSocket + JSON path as the supported ingress contract for `v0.x`.
- Harden the transport contract around:
  - start and stop behavior
  - peer lifecycle reporting
  - transport error semantics
  - message normalization expectations
- Add or refine validation around:
  - malformed JSON
  - missing required fields
  - reconnect behavior
  - duplicate replay across reconnect
- Decide whether transport-side `send()` should handle bigint-backed event fields more directly.

## Documentation

- Keep the README package-facing rather than tutorial-heavy.
- Document the package as:
  - WebSocket + JSON transport
  - normalization boundary before `@causal-order/dedupe`
  - reference ingress layer for the current stack
- Keep validation visible without turning the npm page into a repo workflow guide.

## Expansion Boundary

- Do not expand into additional transport adapters without a real deployment need.
- If another ingress path becomes concrete, decide then whether it belongs as:
  - another adapter in this package, or
  - a separate package such as `@causal-order/transport-kafka`
- Avoid splitting into a transport package family before a second real adapter exists.

## Version Path

- `0.1.1`
  packaging cleanup, README tightening, validation additions, publish readiness
- `0.1.x`
  WebSocket + JSON contract hardening and endurance validation
- `0.2.0`
  only if the public transport behavior or contract meaningfully changes
- `1.0.0`
  once the WebSocket + JSON transport contract is considered stable and long-run validation is routine
