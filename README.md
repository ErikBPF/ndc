# ndc-tpch — Nested Data Compute

Codename: **Ninho de Cobra** ("the snake nest" — nests within nests, and the
harness flushes out the snakes hiding in nested execution).

`ndc-tpch` is a separate derived benchmark over **nested data layouts**:
TPC-H (dbgen data + schema) is its *seed*, never an extension — only the
TPC may extend TPC-H. The suite compares query engines — vanilla Apache
Spark and Apache Spark with [Apache DataFusion Comet](https://github.com/apache/datafusion-comet) —
on flat TPC-H-derived Parquet and nested-layout transforms of the same
data, keeping every answer pinnable so a speedup can never hide a wrong
result.

> The ndc-tpch suite (Ninho de Cobra) is derived from the TPC Benchmark
> TPC-H and as such is not comparable to published TPC-H results, as the
> ndc-tpch results do not comply with the TPC-H Specification.

## Why

Spark and DataFusion Comet are benchmarked on flat TPC-H/TPC-DS only.
Nested types are Comet's active frontier (nested literals, Variant,
nested-field pruning, nested shuffle keys), but nobody measures the
engines when the TPC-H data itself is nested. This harness does, with:

- **Row parity as a first-class invariant** — the flat answer is the
  truth for every layout; a parity failure is a defect record, not a
  result.
- **Fallback accounting** — a run records *what executed* (native vs JVM
  fallback), not only how long.
- **Evaluation blocks** — `core` (native scan+aggregate), `depth`
  (nesting-depth cost sweep 1–8), `manip` (nested manipulation: joins,
  CASE, IN-lists, string ops, HOF quantifiers, top-N).

## Structure

```text
DuckDB tpch dbgen ──> flat Parquet (8 tables) ──> nested transforms
  │                                                ├─ orders_nested: ORDERS + lineitems array<struct>
  │                                                └─ orders_depth1..8: Q6 leaf wrapped d times
  └─ parity reference: the flat answer is the truth for every layout

spark-submit local[8], 3 runs/query, medians, cold-cache discipline
  ├─ vanilla Spark 4.1.3
  └─ Spark + DataFusion Comet (pinned jar)
        └─ results JSON: timings, versions, parity_ok, native/fallback share
```

## Status

POC executed 2026-09-04/05 (50k model validation, 1M, SF 0.5, SF 1, SF 10;
Parquet/Iceberg/Delta × vanilla/Comet; 18/18 cells parity-green). Harness
port from the POC workspace is in progress; see the repository issues.

## Fair use

Benchmarks derived from TPC-H. No comparison with official TPC results is
made or implied; no TPC compliance claim is made; TPC Primary/Optional
Metrics are not used. Engine versions, configurations, and host hardware
are recorded per run.
