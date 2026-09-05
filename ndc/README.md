# ndc-tpch benchmark kit

The stage driver and query set for the ndc-tpch benchmark (see the
[repository README](../README.md) for identity, design, fair-use status,
and disclaimer).

## Layout

| Path | Role |
|---|---|
| `run.sh` | Stage driver: `sanity`, `gen`, `conv`, `nested`, `parity`, `depths`, `setup`, `build-fmt <iceberg\|delta>`, `spark`, `bootstrap <sf> <name>`, `build-scale`, `lite`, `full`, `comet-default`, `report`, `size-check`, `depthsmoke` |
| `spark_poc.py` | Runner: per-query timing, parity vs flat, native/fallback accounting, JSON records |
| `monitor.py` / `merge_monitor.py` | Sidecar `/proc` sampler (tree CPU, peak RSS, IO bytes) merged per query window |
| `write_fmt.py` | Writes the nested table sets to Iceberg / Delta |
| `report.py` | Generates `results/report.md` (per family × format verdicts) |
| `conv.sql`, `nested.sql`, `depths.sql` | Data preparation: flat export → `orders_nested` struct-array transform → depth 1–8 wraps |
| `parity.sql`, `ext_parity.sql` | DuckDB verification suites (built-in + extended pairs) |
| `queries/` | 24 `.sql` files (qgen convention) + manifests: `manifest-full.json` (20-query default kit), `manifest-depth.json` (depth sweep) |
| `answers/sf0.0083/` | Pinned per-query answers at sf 0.0083 (`.out` files) |
| `bench.conf` | Cluster (`CLUSTER=`) and storage (`FS=`) targets |
| `sizes.csv` | Recorded dbgen row counts per scale factor (`size-check` reference) |

## Quickstart

```sh
# one-time: Spark 4.1.3 tarball + Comet/Iceberg/Delta jars under $SPARK41_BASE
#   (run.sh defaults to $HOME/ndc-spark41, override with SPARK41_BASE=...)

./run.sh bootstrap 1 sf1          # new scale workspace under ${WS_ROOT:-$HOME/ndc-workspaces}
cd ${WS_ROOT:-$HOME/ndc-workspaces}/tpch-sf1
./run.sh build-scale              # dbgen -> flat Parquet -> nested -> depths -> Iceberg -> Delta
./run.sh size-check               # assert row counts against ndc/sizes.csv
./run.sh comet-default            # default kit: 20 queries x {vanilla, Comet} x 3 formats,
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

Row parity is enforced at run time: the nested side must agree with the
flat side on every query, and a parity failure is a defect record, not a
timing. The pinned answers in `answers/` are published for independent
verification; wiring them into the automated validity gate is planned.
