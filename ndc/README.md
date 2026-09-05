# ndc-tpch harness

The stage driver and query set for the ndc-tpch benchmark (see the
[repository README](../README.md) for identity, fair-use status, and
disclaimer).

## Layout

| Path | Role |
|---|---|
| `run.sh` | Stage driver: `sanity`, `gen`, `conv`, `nested`, `parity`, `depths`, `setup`, `build-fmt <iceberg\|delta>`, `spark`, `bootstrap <sf> <name>`, `build-scale`, `lite`, `full`, `comet-default`, `report`, `size-check`, `depthsmoke` |
| `spark_poc.py` | spark-submit runner: per-query timing, parity vs flat, native/fallback accounting, JSON records |
| `monitor.py` / `merge_monitor.py` | Sidecar `/proc` sampler (tree CPU, peak RSS, IO bytes) merged per query window |
| `write_fmt.py` | Writes the nested table sets to Iceberg / Delta |
| `report.py` | Generates `results/report.md` (per family × format verdicts) |
| `conv.sql`, `nested.sql`, `depths.sql` | Flat export → `orders_nested` struct-array transform → depth 1–8 wraps |
| `parity.sql`, `ext_parity.sql` | DuckDB parity suites (built-in + extended pairs) |
| `queries_full.json` | The 20-query suite (built-in + depth 1–8 + extended pairs) |
| `queries_depths.json` | Depth-sweep query set |
| `bench.conf` | D11 cluster / D12 storage target (`CLUSTER=`, `FS=`) |
| `sizes.csv` | Recorded dbgen row counts per SF (`size-check` reference) |

## Quickstart

```sh
# one-time: Spark 4.1.3 tarball + Comet/Iceberg/Delta jars under $SPARK41_BASE
#   (run.sh defaults to $HOME/ndc-spark41, override with SPARK41_BASE=...)

./run.sh bootstrap 1 sf1          # new scale workspace under ${WS_ROOT:-$HOME/ndc-workspaces}
cd ${WS_ROOT:-$HOME/ndc-workspaces}/tpch-sf1
./run.sh build-scale              # dbgen -> flat Parquet -> nested -> depths -> Iceberg -> Delta
./run.sh size-check               # assert row counts against ndc/sizes.csv
./run.sh comet-default            # the default kit: 20 queries x {vanilla, Comet} x 3 formats,
                                  # 3 runs, drop-caches, monitor, parity gate, family report
```

Profiles: `lite` (during-work smoke, 1 run, Parquet only, parity gate),
`full` (all formats x engines, 3 runs, drop-caches — release/PR
discipline), `comet-default` (the shipped default kit ending in a family
report). `lite` overwrites `full`-profile JSONs in the same workspace —
keep lite work in a separate workspace.

Cold-cache discipline requires passwordless `sudo` for
`/proc/sys/vm/drop_caches`; runs record `cache: cold|warm|drop-failed`
per query either way.
