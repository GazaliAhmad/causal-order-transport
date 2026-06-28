import assert from "node:assert/strict";
import { mkdtempSync, readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";

const execFileAsync = promisify(execFile);

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const runtimeBin = resolve(
  repoRoot,
  "node_modules",
  "@causal-order",
  "testing",
  "bin",
  "causal-order-testing-runtime.js",
);
const latestBin = resolve(
  repoRoot,
  "node_modules",
  "@causal-order",
  "testing",
  "bin",
  "causal-order-testing-latest.js",
);

const harnessRoot = mkdtempSync(join(tmpdir(), "causal-order-testing-"));

await execFileAsync(
  process.execPath,
  [
    runtimeBin,
    "--duration",
    "20s",
    "--steady-for",
    "10s",
    "--time-scale",
    "100",
    "--events-per-second",
    "4",
    "--profile",
    "expected-production-3way-mesh",
    "--run-name",
    "transport-smoke",
  ],
  {
    cwd: harnessRoot,
  },
);

const runsDir = resolve(harnessRoot, "artifacts", "runs");
const runFolders = readdirSync(runsDir, { withFileTypes: true })
  .filter((entry) => entry.isDirectory())
  .map((entry) => entry.name);

assert.equal(runFolders.length, 1, "expected exactly one harness run folder");

const summaryPath = resolve(runsDir, runFolders[0], "summary.json");
const summary = JSON.parse(readFileSync(summaryPath, "utf8"));

assert.equal(summary.outcome?.status, "completed", "harness run should complete");
assert.ok(Number(summary.stream?.orderedEvents ?? 0) > 0, "harness should produce ordered events");
assert.ok(Number(summary.simulation?.generated ?? 0) > 0, "harness should generate traffic");

const latestResult = await execFileAsync(process.execPath, [latestBin], {
  cwd: harnessRoot,
});

assert.match(latestResult.stdout, /status=completed/, "latest summary should report completion");
assert.match(latestResult.stdout, /ordered=/, "latest summary should include ordered event count");

process.stdout.write(
  `Testing harness smoke passed with ${summary.stream.orderedEvents} ordered events in ${harnessRoot}\n`,
);
