# Running NDC

NDC is TPC-H-derived and TPC-DS-inspired; see the [root README](../README.md) and
[methodology](../docs/methodology.md) for scope and comparison rules.

Run commands from the repository root. `run.sh` enters the locked Nix environment.
`NDC_WORKSPACE` selects data/configuration; code stays in the pinned checkout.

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
# First select an already prepared workspace (see the root quickstart).
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

### Outputs larger than driver memory

Use `VALIDATION=distributed ./ndc/run.sh matrix`. This preserves complete-row and
duplicate checks on executors. Read timing includes Python serialization and disk
materialization; compare only campaigns using the same validation mode. Default
`collect` timing remains available. See [measurement rules](../docs/methodology.md#distributed-validation).

`tests/spark_validation.py` runs under `spark-submit` in CI and verifies exact bag
and ordered comparison, stable answer identities, write round trips without driver
collection, and validation above a 1 MiB driver result limit.

See [the TPC-DS interface comparison](../docs/test-interface.md) for phase mappings
and current interface gaps.

### Frozen execution schedules

Every matrix or phase command writes `plan.json` before its first Spark launch.
It contains resolved query/reference SQL, phase, stream model, query seed, warm-ups and
ordered samples for every repetition and stream. Cells embed that plan and its
identity; reporting verifies both the plan and observed execution order. Custom
SQL literals are frozen as written; no query parameter generator is implied.
`plan <out.json>` previews the same matrix schedule without executing it. Relative plan output paths
resolve inside `NDC_WORKSPACE`. Serial
`latency` and `maintenance` enforce one stream; concurrent phases reject writes.
