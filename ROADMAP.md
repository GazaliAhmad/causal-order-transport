# Roadmap

## Direction

`@causal-order/transport` is currently the WebSocket + JSON transport layer for the `causal-order` stack.

The current goal is to keep hardening that ingress path as a deployable runtime layer before expanding into additional transport-specific adapters.

The package is intended to sit here:

```text
WebSocket + JSON -> @causal-order/transport -> @causal-order/dedupe -> causal-order
```

## Current Baseline

- `0.1.1` is the publish baseline for the current package state.
- The WebSocket + JSON path has completed `2h`, `4h`, and `8h` wall-clock validation runs.
- The heap-tracked `8h` rerun is the current memory baseline after the harness memory-retention fix.
- The current validation record supports the package as a deployable ingress layer for the present stack shape.
- Keep the npm package surface small and runtime-focused.

## Next Work

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
- Keep confirming long-run behavior on:
  - bounded dedupe cache behavior
  - anomaly severity distribution
  - final `accepted == ordered` completion behavior
  - stable memory behavior with the corrected harness
- Optionally run `12h` only if longer endurance characterization is still useful after the corrected `8h` memory profile.
- Decide whether transport-side `send()` should handle bigint-backed event fields more directly.

## Documentation

- Keep the README package-facing rather than tutorial-heavy.
- Document the package as:
  - WebSocket + JSON transport
  - normalization boundary before `@causal-order/dedupe`
  - deployable ingress layer for the current stack
- Keep validation visible without turning the npm page into a repo workflow guide.

## Expansion Boundary

- Do not expand into additional transport adapters without a real deployment need.
- If another ingress path becomes concrete, decide then whether it belongs as:
  - another adapter in this package, or
  - a separate package such as `@causal-order/transport-kafka`
- Avoid splitting into a transport package family before a second real adapter exists.

## Version Path

- `0.1.1`
  packaging cleanup, npm-facing docs, wall-clock validation, heap tracking, and harness memory-retention fix
- `0.1.x`
  WebSocket + JSON contract hardening and any follow-up memory/operational tuning
- `0.2.0`
  only if the public transport behavior or contract meaningfully changes
- `1.0.0`
  once the WebSocket + JSON transport contract is considered stable and long-run validation is routine
