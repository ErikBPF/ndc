# Running NDC with Spark

NDC is TPC-H-derived and TPC-DS-inspired; see the [root README](../README.md) and
[methodology](../docs/methodology.md) for scope and comparison rules.

This guide covers the bundled Spark runner, including Comet. Its CLI choices and
local deployment limits describe this implementation, not the benchmark contract.
See [other engines](../docs/engines.md) for porting requirements and implementation gaps.

Run commands from the repository root. `run.sh` enters the locked Nix environment.
`NDC_WORKSPACE` selects data/configuration; code stays in the pinned checkout.

Current runner: Spark 4.1.3, with optional DataFusion Comet 1.0.0.
Implemented formats: Parquet, Iceberg, Delta. Flat and nested inputs use **matched formats** by
default; `LAYOUT_MODE=mixed` reproduces the former flat-Parquet control.
Parquet update/delete are unsupported, not zero-duration successes.

## Quickstart (Linux x86-64)

The bundled Spark runner requires Nix with flakes enabled, network access for initial tool/artifact downloads,
and sufficient local disk. The locked Nix shell supplies Python, DuckDB, Java and
ShellCheck. Engine downloads have pinned checksums in `ndc/artifacts.json`.
Setup tries Apache’s CDN, then its download server and Archive, abandoning
stalled or slow transfers and checking the pinned checksum before installation.
Checksum mismatches fail immediately; they do not trigger a different source.
CI caches only downloaded archives/JARs under the artifact-lock identity, verifies
them again during setup, and extracts the runtime afresh. Extracted runtime trees
are not restored from the CI cache.

```sh
./ndc/run.sh check
./ndc/run.sh setup
./ndc/run.sh bootstrap 0.0083 tiny
export NDC_WORKSPACE="$PWD/workspaces/tpch-tiny"
./ndc/run.sh build-scale
./ndc/run.sh qualify-engine vanilla parquet # all 46 cases plus prerequisite gates
nix develop "path:$PWD/nix" -c python3 tests/integration.py
```

Defaults: `local[4]`, 8 GiB driver heap, 128 synthetic parents, maximum fan-out 64,
8 padding fields, seed 7. Comet additionally uses a configured 2 GiB off-heap pool;
record allocated CPU/process memory limits when comparing engines. Use `SPARK_MASTER` and
`SPARK_DRIVER_MEM` to fit your machine. The tiny qualification scale is intentionally
not a TPC publication scale.

Use `SUITE=tpch ./ndc/run.sh latency` for serial reads,
`./ndc/run.sh shared-throughput` for shared-session concurrency, and
`./ndc/run.sh maintenance` for update/delete/compaction. `matrix` compares selected
engine/format cells. `test`, `full`, `comet-default`, `throughput`, and `qualify`
remain compatibility aliases; prefer the explicit commands above.

Every campaign freezes its SQL and execution order in `plan.json` before Spark
starts. Reports reject missing, changed or out-of-order planned samples.
Every campaign has its own directory under `$NDC_WORKSPACE/results/`. Existing
result files are never overwritten. `lite` cannot replace a previous full campaign.
Sources execute from this checkout; workspaces contain data/configuration, not stale
copies of the harness. Keep a pinned checkout or a complete source bundle with results.

## Commands

`./ndc/run.sh --help` lists entry points without entering Nix or reading workspace
configuration. `check` runs repository checks without sourcing `bench.conf`.
Workspace paths are resolved before child commands run. Benchmark execution still
sources `bench.conf`; use only trusted workspaces and candidate artifacts.


| Command | Behavior |
|---|---|
| `./ndc/run.sh setup` | Download and verify Spark/Comet/Iceberg/Delta artifacts |
| `./ndc/run.sh bootstrap <sf> <name>` | Create a new workspace; reject existing names |
| `./ndc/run.sh build-scale` | Generate, size-check, export, nest, validate composition, qualify tiny answers, build depths/shapes, prepare formats |
| `./ndc/run.sh shapes` | Build synthetic shapes in a fresh data directory |
| `./ndc/run.sh build-fmt iceberg` | Prepare matched flat and nested Iceberg tables; Delta analogous |
| `./ndc/run.sh size-check` | Check all eight TPC-H table counts against the declared scale |
| `./ndc/run.sh invariants` | Validate exact nested-leaf bags and parent membership in DuckDB |
| `./ndc/run.sh parity` | Compare flat and nested reference results in DuckDB |
| `./ndc/run.sh qualify-references` | Check the 24 tiny pins using eight independent DuckDB flat queries |
| `./ndc/run.sh qualify-engine <engine> [fmt]` | On prepared sf0.0083 data: sizing, structural invariants, reference pins, parity, then all 46 candidate workloads; one `qualification.json` verdict |
| `./ndc/run.sh lite` | One repetition of historical query membership on Parquet |
| `./ndc/run.sh matrix` | Selected suite/custom manifest across formats, engines and repetitions |
| `./ndc/run.sh latency` | Serial read/compute measurement; defaults to `SUITE=read` |
| `./ndc/run.sh maintenance` | Serial update/delete/compaction; defaults to `SUITE=maintenance` |
| `./ndc/run.sh plan <out.json>` | Preview a frozen matrix schedule from the selected suite/settings; reject an existing file |
| `./ndc/run.sh full` / `comet-default` | Aliases for the full matrix driver |
| `./ndc/run.sh shared-throughput` | Concurrent read streams; defaults to the DS-inspired suite |
| `./ndc/run.sh report <campaign>` | Validate compatibility and produce a report |
| `./ndc/run.sh bundle <campaign>` | Archive diagnostics, measured source, metadata and checksums |
| `./ndc/run.sh check` | Standard-library regression tests, ShellCheck, Bash syntax |

`test` aliases `check`; `qualify` aliases `qualify-references`; `throughput` aliases
`shared-throughput`. `full` and `comet-default` alias `matrix` (default all 46 cases).
The historical 24-case suite is now named `tpch`; `SUITE=full` remains its legacy
membership alias. Command names and suite names are separate.

Candidate qualification requires an already prepared tiny workspace. It fixes one
repetition, one stream, no warm-up, uncontrolled cache and all 46 workloads for the
selected candidate. `NDC_CAMPAIGN_DIR`, if provided, names a fresh qualification
root; its engine campaign lives under `campaign/`. Bundle that child campaign to
include the qualification summary and gate logs. Each gate has a log, and failures
stop subsequent gates. Supported engines are `vanilla` and `comet`; the default
format is `parquet`, with `iceberg` and `delta` also accepted. A successful verdict
permits explicitly unsupported capabilities, including Parquet UPDATE/DELETE;
inspect the campaign report for exclusions. Larger-scale experiments use `matrix`
or the phase commands.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `SPARK41_BASE` | `$HOME/ndc-spark41` | Verified distribution and JAR directory |
| `SPARK_MASTER` | `local[4]` | Spark execution target; only local host orchestration supported |
| `SPARK_DRIVER_MEM` | `8g` | Driver heap; also disclose native/off-heap limits |
| `FORMATS` | `parquet iceberg delta` | Space-separated format cells |
| `ENGINES` | `vanilla comet` | Engine cells; default ordering alternates by format |
| `RUNS` | `3` | Measured repetitions for matrix and measurement phases; qualification fixes one |
| `WARMUPS` | `1` | Validated untimed executions per workload |
| `VALIDATION` | `collect` | `distributed` validates full outputs on executors; timing modes cannot be mixed |
| `CACHE` | `uncontrolled` | `uncontrolled`, `warm`, or explicit `cold` |
| `LAYOUT_MODE` | `matched` | `mixed` keeps flat references as Parquet |
| `DATA_SEED` | `SEED` or `7` | Synthetic generation seed; does not alter DuckDB TPC-H dbgen |
| `QUERY_SEED` | `SEED` or `7` | Frozen query-permutation seed; SQL parameters stay fixed |
| `SEED` | `7` | Legacy fallback for both independent seeds |
| `STREAMS` | `1` (`2` for shared-throughput) | Concurrent read streams in one Spark application |
| `PARENTS`, `FANOUT`, `WIDTH` | `128`, `64`, `8` | Synthetic shape parameters, used during build |
| `SUITE` | `all` for matrix | `tpch`, `read`, `all`, `scan`, `compute`, `depth`, `shapes`, `ds`, `write`, `maintenance` |
| `QUERIES` | Unset | Optional absolute custom manifest path; overrides `SUITE` |
| `NDC_CAMPAIGN_DIR` | Unique timestamp/UUID path | Optional explicit fresh output directory |

`COMET_JAR`, `ICEBERG_JAR`, `DELTA_JAR`, `DELTA_STORAGE_JAR` accept deliberate
candidate overrides. Results record the actual hashes; candidate artifacts must
be provided before execution. Do not silently relabel them as the default release.
`bench.conf` only declares implemented local execution/storage. Credentials are
not needed; remote storage and cluster deployment are not implemented here.

## Examples

```sh
# First select an already prepared workspace (see Quickstart above).
export NDC_WORKSPACE="$PWD/workspaces/tpch-tiny"

# Preview query order without launching Spark; output is workspace-relative.
SUITE=read QUERY_SEED=11 ./ndc/run.sh plan read-plan.json

# Correctness campaign, bounded input; no performance claim.
RUNS=1 WARMUPS=0 ./ndc/run.sh matrix

# Repeated materialized-read/compute experiment.
SUITE=shapes RUNS=5 CACHE=warm ./ndc/run.sh latency

# A distinct shared-application throughput experiment.
SUITE=ds RUNS=3 STREAMS=2 WARMUPS=1 ./ndc/run.sh shared-throughput

# Writes and maintenance are separate selectable suites.
SUITE=write ./ndc/run.sh matrix
./ndc/run.sh maintenance

# Run after the tiny workspace has been built; expects deliberately wrong SQL to fail.
nix develop "path:$PWD/nix" -c python3 tests/integration.py
```

For scale/shape sweeps, bootstrap a different workspace for each point and set its
build parameters. Regenerating shape files in place is rejected. Rebuilding table
formats intentionally replaces their prepared copies and records a new physical
identity. A stale or altered format copy fails preflight before timing.

## CI

`ci.yml` runs harness checks and candidate qualification on each PR/push, in collect
and distributed modes for vanilla/Parquet.
Scheduled/manual runs also qualify Comet, Iceberg and Delta. Failures preserve raw
logs and result artifacts. `security.yml` scans PRs/pushes and can run manually.
Actions are pinned to immutable commits; Nix and JVM artifacts are pinned too.
Performance thresholds belong on controlled hardware, not hosted CI runners.
The workflow does not deploy infrastructure, publish benchmark claims, or run
host-wide cache drops.

## Outputs larger than driver memory

Use `VALIDATION=distributed ./ndc/run.sh matrix`. This preserves complete-row and
duplicate checks on executors. Read timing includes Python serialization and disk
materialization; compare only campaigns using the same validation mode. Default
`collect` timing remains available. See [measurement rules](../docs/methodology.md#distributed-validation).

`tests/spark_validation.py` runs under `spark-submit` in CI and verifies exact bag
and ordered comparison, stable answer identities, write round trips without driver
collection, and validation above a 1 MiB driver result limit.

See [the TPC-DS interface comparison](../docs/test-interface.md) for phase mappings
and interface gaps.

## Frozen execution schedules

Every matrix or phase command writes `plan.json` before its first Spark launch.
It contains resolved query/reference SQL, phase, stream model, query seed, warm-ups and
ordered samples for every repetition and stream. Cells embed that plan and its
identity; reporting verifies both the plan and observed execution order. Custom
SQL literals are frozen as written; no query parameter generator is implied.
`plan <out.json>` previews the same matrix schedule without executing it. Relative plan output paths
resolve inside `NDC_WORKSPACE`. Serial
`latency` and `maintenance` enforce one stream; concurrent phases reject writes.

## Candidate experiments

Save this specification as `candidates.json` in the repository root, replacing
artifact paths and revisions with the reviewed builds:

```json
{
  "candidates": [
    {
      "label": "base",
      "command": ["./ndc/run.sh", "latency"],
      "env": {"ENGINES": "comet", "COMET_JAR": "/path/to/base.jar"},
      "revision": "BASE_REVISION",
      "build_profile": "release"
    },
    {
      "label": "candidate",
      "command": ["./ndc/run.sh", "latency"],
      "env": {"ENGINES": "comet", "COMET_JAR": "/path/to/candidate.jar"},
      "revision": "CANDIDATE_REVISION",
      "build_profile": "release"
    }
  ]
}
```

Use absolute workspace and artifact paths. Each candidate must select one engine
and one format. Prepare and qualify the data/candidates before an experiment.

```sh
export NDC_WORKSPACE="$PWD/workspaces/tpch-validation"
export FORMATS=iceberg SUITE=read
export NDC_CPU_LIMIT=4 NDC_MEMORY_LIMIT=16G
./ndc/run.sh experiment candidates.json --out experiment --mode performance \
  --operator CometIcebergNativeScan \
  --require-operator candidate q1_nested_explode CometIcebergNativeScan
```

The performance default for two candidates is four passes, each using a fresh
process, one validated warmup per query and one timed sample. Candidate positions
are balanced across passes. The CPU and memory settings use native systemd user
scopes; a missing/unavailable systemd user manager fails the command instead of
silently running uncapped. CPU quota is not CPU pinning. Omit these settings when
resource limits are already enforced by your execution environment. Effective
cgroup limits, execution threads, driver heap and off-heap settings are recorded.
`SPARK_MASTER`, `SPARK_DRIVER_MEM` and the existing runner configuration retain
their meanings; a memory cap does not automatically resize the driver heap.

Ordinary commands default to `NDC_RUN_INTENT=correctness`. Candidate label,
revision and profile can also be supplied with `NDC_CANDIDATE`,
`NDC_CANDIDATE_REVISION`, and `NDC_BUILD_PROFILE`. These are declared metadata;
JAR checksums are computed independently. Progress shows stages, warmups, and
per-stream query completion while full runtime output remains in the cell log.

`compare` and `experiment` resolve CLI paths from the caller's directory and do
not source workspace `bench.conf`. Their candidate engine commands still load
that trusted configuration. See [comparison rules](../docs/experiments.md).
