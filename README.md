# NDC — Nested Data Compute

**Ninho de Cobra** is an engine-agnostic benchmark for scanning, materialized reads,
nested computation, writes, and maintenance. Workload semantics and correctness
contracts apply across engines. The current implementation targets Spark, with
vanilla Spark and Comet execution modes; additional engines can implement the same
contracts using their own SQL dialect and execution model. Its relational baseline and historical `ndc-tpch` suite
are **derived from TPC-H**. Its qualification discipline, richer analytical
workloads, and maintenance/throughput separation are **inspired by TPC-DS**.
Synthetic shape fixtures complement the TPC-H data; they do not alter its entities.

> NDC is derived from the TPC Benchmark TPC-H and is not comparable to published
> TPC-H results; it does not comply with the TPC-H Specification. NDC also draws
> inspiration from TPC-DS and is not a compliant TPC-DS implementation. Its results
> are not comparable to published TPC-DS results. No TPC performance metric is used.

## What runs

| Suite | Cases | Purpose |
|---|---:|---|
| `tpch` | 24 | Historical TPC-H-derived query membership; corrected complete validation and top-N semantics |
| `scan` / `compute` / `depth` | subsets of `tpch` | Projection controls, manipulation, and singleton-wrapper depth isolation |
| `shapes` | 13 | Leaf/multiple/full projection, selectivity, nulls, fan-out, branching, maps, zipped arrays, quantifiers, regrouping, top-N |
| `ds` | 2 | Dimension join + ranking; sales/returns collections + union + rollup |
| `write` | 4 | Fresh materialization, nested transformation, flat-to-nested construction, append |
| `maintenance` | 3 | Update, delete, compaction; unsupported operations explicitly recorded |
| `read` | 39 | All read/compute cases, excluding writes and maintenance |
| `all` | 46 | All of the above, without counting subset manifests twice |

Current runner: Spark 4.1.3, with optional DataFusion Comet 1.0.0.
Implemented formats: Parquet, Iceberg, Delta. Flat and nested inputs use **matched formats** by
default; `LAYOUT_MODE=mixed` reproduces the former flat-Parquet control.
Parquet update/delete are unsupported, not zero-duration successes.

## Spark quickstart (Linux x86-64)

The bundled Spark runner requires Nix with flakes enabled, network access for initial tool/artifact downloads,
and sufficient local disk. The locked Nix shell supplies Python, DuckDB, Java and
ShellCheck. Engine downloads have pinned checksums in `ndc/artifacts.json`.

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

## Validity before comparison

Every timed repetition is checked, including complete rows, duplicate multiplicity,
nulls, and declared ordering. The tiny TPC-H suite checks pinned answers; DuckDB
independently checks those pins against flat reference SQL. Larger scales use
explicit flat/nested reference comparisons. Synthetic full reads have a separate
Python oracle; other synthetic queries use equivalent reference SQL. Writes reopen
committed output and compare complete contents; maintenance checks expected state.

Failed, missing, unvalidated, stale, or incompatible results cannot receive a
speedup comparison. Raw failure records remain available. Reports show per-query
medians, sample counts/ranges, and descriptive family totals, without an overall
weighted score or an automatic significance verdict.

## Documentation

- [Benchmark contract and other engines](docs/engines.md)

- [Workloads and schemas](docs/workloads.md)
- [Measurement and validity rules](docs/methodology.md)
- [Commands and CI](ndc/README.md)
- [Test interface compared with TPC-DS](docs/test-interface.md)
- [Answer provenance](ndc/answers/README.md)
- [Validation and CI](docs/validation.md)
- [Attribution and deviations](NOTICE)

Performance claims must identify the measured implementation and link a complete
immutable artifact bundle, including source identity, raw samples, plans and
checksums. Results from another harness revision require qualification under that
revision’s measurement and validity rules.
