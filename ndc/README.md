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
| `./ndc/run.sh qualify` | Check the 24 tiny pins using eight independent DuckDB flat queries |
| `./ndc/run.sh lite` | One repetition of historical query membership on Parquet |
| `./ndc/run.sh matrix` | Selected query manifest, formats, engines, repetitions |
| `./ndc/run.sh full` / `comet-default` | Aliases for the full matrix driver |
| `./ndc/run.sh throughput` | Concurrent read streams; defaults to the DS-inspired suite |
| `./ndc/run.sh report <campaign>` | Validate compatibility and produce a report |
| `./ndc/run.sh bundle <campaign>` | Archive diagnostics, measured source, metadata and checksums |
| `./ndc/run.sh test` | Standard-library regression tests, ShellCheck, Bash syntax |

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `SPARK41_BASE` | `$HOME/ndc-spark41` | Verified distribution and JAR directory |
| `SPARK_MASTER` | `local[4]` | Spark execution target; only local host orchestration supported |
| `SPARK_DRIVER_MEM` | `8g` | Driver heap; also disclose native/off-heap limits |
| `FORMATS` | `parquet iceberg delta` | Space-separated format cells |
| `ENGINES` | `vanilla comet` | Engine cells; default ordering alternates by format |
| `RUNS` | `3` | Measured repetitions for matrix/full/throughput |
| `WARMUPS` | `1` | Validated untimed executions per workload |
| `CACHE` | `uncontrolled` | `uncontrolled`, `warm`, or explicit `cold` |
| `LAYOUT_MODE` | `matched` | `mixed` keeps flat references as Parquet |
| `SEED` | `7` | Data generation and query-permutation seed |
| `STREAMS` | `1` (`2` for throughput) | Concurrent read streams in one Spark application |
| `PARENTS`, `FANOUT`, `WIDTH` | `128`, `64`, `8` | Synthetic shape parameters, used during build |
| `QUERIES` | `manifest-all.json` for matrix | Absolute path to an explicit query manifest |
| `NDC_CAMPAIGN_DIR` | Unique timestamp/UUID path | Optional explicit fresh output directory |

`COMET_JAR`, `ICEBERG_JAR`, `DELTA_JAR`, `DELTA_STORAGE_JAR` accept deliberate
candidate overrides. Results record the actual hashes; candidate artifacts must
be provided before execution. Do not silently relabel them as the default release.
`bench.conf` only declares implemented local execution/storage. Credentials are
not needed; remote storage and cluster deployment are not implemented here.

## Examples

```sh
# Correctness campaign, bounded input; no performance claim.
RUNS=1 WARMUPS=0 ./ndc/run.sh matrix

# Repeated materialized-read/compute experiment.
QUERIES="$PWD/ndc/queries/manifest-shapes.json" RUNS=5 CACHE=warm ./ndc/run.sh matrix

# A distinct shared-application throughput experiment.
RUNS=3 STREAMS=2 WARMUPS=1 ./ndc/run.sh throughput

# Writes and maintenance are separate selectable suites.
QUERIES="$PWD/ndc/queries/manifest-write.json" ./ndc/run.sh matrix
QUERIES="$PWD/ndc/queries/manifest-maintenance.json" ./ndc/run.sh matrix

# Run after the tiny workspace has been built; expects deliberately wrong SQL to fail.
nix develop "path:$PWD/nix" -c python3 tests/integration.py
```

For scale/shape sweeps, bootstrap a different workspace for each point and set its
build parameters. Regenerating shape files in place is rejected. Rebuilding table
formats intentionally replaces their prepared copies and records a new physical
identity. A stale or altered format copy fails preflight before timing.

## CI

`ci.yml` runs harness checks and a tiny real Spark/Parquet campaign on each PR/push.
Scheduled/manual runs also qualify Comet, Iceberg and Delta. Failures preserve raw
logs and result artifacts. `security.yml` scans PRs/pushes and can run manually.
Actions are pinned to immutable commits; Nix and JVM artifacts are pinned too.
Performance thresholds belong on controlled hardware, not hosted CI runners.
The workflow does not deploy infrastructure, publish benchmark claims, or run
host-wide cache drops. GitHub execution requires pushing the workflow changes.
