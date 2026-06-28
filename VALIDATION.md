# Validation

`@causal-order/transport` is validated around the current supported ingress path:

```text
WebSocket + JSON -> @causal-order/transport -> @causal-order/dedupe -> causal-order
```

## Test Layers

- [`npm run test:smoke`](https://github.com/GazaliAhmad/causal-order-transport/blob/main/scripts/smoke.mjs)
  basic normalization verification
- [`npm run test:multi`](https://github.com/GazaliAhmad/causal-order-transport/blob/main/scripts/multi-node.mjs)
  concurrent 12-node ingress verification
- [`npm run test:e2e`](https://github.com/GazaliAhmad/causal-order-transport/blob/main/scripts/e2e-pipeline.mjs)
  `transport -> @causal-order/dedupe -> causal-order` handoff verification
- [`npm run test:harness`](https://github.com/GazaliAhmad/causal-order-transport/blob/main/scripts/testing-harness-smoke.mjs)
  smoke verification against `@causal-order/testing`

## Wall-Clock Runs

Long-running wall-clock validation is implemented in [`scripts/wallclock-transport-runtime.mjs`](https://github.com/GazaliAhmad/causal-order-transport/blob/main/scripts/wallclock-transport-runtime.mjs).

Available runs:

- `npm run test:wallclock:2h`
- `npm run test:wallclock:4h`
- `npm run test:wallclock:8h`
- `npm run test:wallclock:12h`

These runs use the `typical-real-world-mesh` workload profile across 8 WebSocket nodes.

Tracked outputs include:

- generated, received, accepted, dropped, and ordered event counts
- anomaly types and severities
- transport latency
- dedupe filter latency
- end-to-end ordering latency
- heartbeat memory snapshots plus `maxRssBytes`, `finalRssBytes`, and `avgRssBytes`
- heap tracking for `heapUsedBytes`, `heapTotalBytes`, `externalBytes`, and `arrayBuffersBytes`
- wall-clock start, stop, and drain timing

Artifacts are written under `artifacts/transport-runs/`.

## Observed Results

- `2h`
  completed with `generated = accepted = ordered = 124941`, `droppedDuplicates = 435`, warnings only, and RSS roughly stable in the high-60 MB to low-70 MB range
- `4h`
  completed with `generated = accepted = ordered = 249771`, `droppedDuplicates = 849`, warnings only, and RSS stable with `avgRssBytes = 66100339` and `maxRssBytes = 71872512`
- `8h`
  completed with `generated = accepted = ordered = 501128`, `droppedDuplicates = 1613`, warnings only, `avgRssBytes = 84308196`, and `maxRssBytes = 95625216`

These runs support the longer `8h` validation step because:

- the `4h` run preserved the same completion behavior as the `2h` run
- anomaly severity remained warning-only
- memory stayed bounded
- queue depth and dedupe cache behavior remained stable

The completed `8h` run extends that validation story, but it also shows a clearer RSS rise than the `2h` and `4h` runs. The run still completed cleanly with:

- `transport.errors = 0`
- bounded dedupe cache behavior
- low queue depth
- warning-only anomalies
- final `accepted = ordered`

That result does not prove a leak by itself, because RSS alone cannot distinguish a true heap leak from runtime memory retention.

It is also important to note that the earlier wall-clock harness retained full latency sample arrays in memory for the duration of the run. That meant the harness itself could contribute materially to long-run RSS growth, especially in the `8h` run.

The wall-clock harness has since been updated so latency tracking uses running summaries rather than retaining full in-memory sample arrays. That change reduces harness-side memory pressure and makes later endurance runs more useful for diagnosing the actual transport/runtime path.

The next diagnostic step is a fresh `8h` rerun with the corrected harness and the additional memory tracking now recorded by the wall-clock runtime:

- `rssBytes`
- `heapUsedBytes`
- `heapTotalBytes`
- `externalBytes`
- `arrayBuffersBytes`

That rerun is intended to determine whether the observed long-run RSS rise reflects:

- a small heap leak
- external or buffer retention
- or normal long-lived runtime memory behavior
