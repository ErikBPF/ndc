# ndc-tpch — Nested Data Compute

`ndc-tpch` is a benchmark over **nested data layouts**, seeded by
TPC-H (dbgen data + schema). It measures query engines on flat Parquet
and nested-layout transforms of the same data, and keeps every answer
pinnable so a speedup can never hide a wrong result. **Ninho de Cobra**
— the snake nest — for my Brazilian friends.

> The ndc-tpch suite is derived from the TPC Benchmark
> TPC-H and as such is not comparable to published TPC-H results, as the
> ndc-tpch results do not comply with the TPC-H Specification.

Reference engines: vanilla Apache Spark 4.1.3 and Apache Spark with
[Apache DataFusion Comet](https://github.com/apache/datafusion-comet).
Any engine that speaks Spark SQL can be added behind the same
record schema.

## Data

TPC-H data is generated with dbgen (via the DuckDB `tpch` extension) at a
declared scale factor and exported to flat Parquet. Nested layouts are
derived from the same entities by transform — no row is added, dropped,
or altered; per-order composition is preserved.

| Layout | Shape |
|---|---|
| flat | 8 TPC-H tables as exported |
| `orders_nested` | ORDERS + `lineitems array<struct>` (12-col leaf) |
| `orders_depth1..8` | Q6 leaf wrapped 1–8 times, alternating LIST/STRUCT |

Scale factors are recorded with exact dbgen row counts in
[`ndc/sizes.csv`](ndc/sizes.csv) and asserted at build time
(`size-check`). The table-format axis (Parquet, Iceberg, Delta) applies
to the nested side; the flat reference always reads Parquet.

## Queries

24 result queries in three families. Every nested variant declares the
nested operation it stresses, and every query's flat and nested sides
must agree before any timing counts (row parity).

| Family | Queries | Nested operation exercised |
|---|---|---|
| built-in (scan+agg) | 4 | flat scan + filter + group-agg; struct-array scan/unnest; pure higher-order aggregation |
| depth 1–8 | 8 | the same Q6 leaf behind 1–8 alternating wrappers — the cost of nesting depth itself |
| extended | 12 (6 pairs) | joins, CASE, IN-lists, string ops, correlated quantifiers as HOFs, top-N (window vs nested sort) |

Query text lives in [`ndc/queries/`](ndc/queries/) as individual `.sql`
files with manifests per set (`manifest-full.json`, the full 24-query
set; `manifest-depth.json`, the depth sweep) — the qgen convention of
TPC-H.

## Answers

Pinned qualification answers ship per scale factor in
[`ndc/answers/`](ndc/answers/) (`.out` files, one per query, pipe
separated) for independent verification; wiring them into the automated
validity gate is part of the planned evaluation-block API. Row parity
is enforced at run time; a parity failure is recorded as a defect, not
a timing.

## Metrics

Per query and cell, the record carries: wall-time median over 3 runs,
native/fallback share (a query that fell back to JVM execution is
marked), row parity, CPU seconds, peak RSS, and cold-cache IO bytes
(`cache: cold|warm|drop-failed` recorded per run; page cache is dropped
between runs). Reported speedups are ratios of medians.

## Fair use and disclosure

Results carry the prescribed disclaimer above wherever they are
presented. No TPC Primary or Optional Metric is used; engine versions,
configurations, and host capacity are recorded per run; and all
deviations from the TPC-H Specification (schema layout, query text, run
methodology, metrics) are declared in this repository and in each result
record. See [NOTICE](NOTICE).

## Running

[`ndc/README.md`](ndc/README.md) documents the stage driver and the
three profiles: `lite` (smoke, not citable), `full` (release
discipline), `comet-default` (the shipped default kit ending in a
generated family report).

## Structure

```text
ndc/
  run.sh            stage driver: build, profiles, size-check, report
  spark_poc.py      runner: timing, parity, native/fallback accounting, JSON records
  monitor.py        sidecar /proc sampler (tree CPU, peak RSS, IO bytes)
  write_fmt.py      writes the nested table sets to Iceberg / Delta
  report.py         per family × format verdict report
  conv.sql, nested.sql, depths.sql     data preparation transforms
  parity.sql, ext_parity.sql           DuckDB verification suites
  queries/          24 .sql files + manifests (qgen convention)
  answers/          pinned per-query answers per scale factor
  bench.conf        cluster (D11) and storage (D12) targets
  sizes.csv         recorded dbgen row counts per scale factor
```

## Results so far

First campaign executed on a single 28-core host with local NVMe storage:
sf 0.5, sf 1, sf 10 — Parquet / Iceberg / Delta × vanilla / Comet, all
cells parity-green.

Total speedup, 24 queries, medians of 3 cold-cache runs:

| Total speedup | sf 0.5 | sf 1 | sf 10 |
|---|---:|---:|---:|
| Parquet | 1.20x | 1.18x | 1.27x |
| Iceberg | 1.27x | 1.16x | 1.21x |
| Delta | 1.14x | 1.05x | 1.13x |

Per family on Parquet (vanilla → Comet):

| Family | sf 0.5 | sf 1 | sf 10 |
|---|---:|---:|---:|
| built-in (scan+agg) | 1.67x | 2.04x | 2.47x |
| depth 1–8 | 1.24x | 1.13x | 1.12x |
| extended (manipulation) | 1.06x | 1.00x | 1.08x |

The stable findings: Comet's advantage concentrates in native
scan+aggregate and compounds with volume; the nested-manipulation family
is its weak spot at any volume; fallback-flattened cells hide
native-execution effects. Raw result records and the analysis live in
the coordinating repository.
