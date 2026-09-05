# ndc-tpch — Nested Data Compute

Codename: **Ninho de Cobra**.

`ndc-tpch` is a separate derived benchmark over **nested data layouts**:
TPC-H (dbgen data + schema) is its seed. The suite measures query
engines on flat TPC-H-derived Parquet and nested-layout transforms of
the same data, and keeps every answer pinnable so a speedup can never
hide a wrong result.

> The ndc-tpch suite (Ninho de Cobra) is derived from the TPC Benchmark
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
files with manifests per set (`manifest-full.json`, the 20-query
default kit; `manifest-depth.json`, the depth sweep) — the qgen
convention of TPC-H.

## Answers

Pinned qualification answers ship per scale factor in
[`ndc/answers/`](ndc/answers/) (`.out` files, one per query, pipe
separated). Row parity is enforced at run time — the nested side must
agree with the flat side on every query before a timing counts, and a
parity failure is recorded as a defect, not a result. The pinned answers
are published for independent verification; wiring them into the
automated validity gate is part of the planned evaluation-block API.

## Metrics

Per query and cell, the record carries: wall-time median over 3 runs,
native/fallback share (a query that fell back to JVM execution is
marked), row parity, CPU seconds, peak RSS, and cold-cache IO bytes
(`cache: cold|warm|drop-failed` recorded per run; page cache is dropped
between runs). Reported speedups are ratios of medians — no TPC
Primary or Optional Metric is used.

## Fair use and disclosure

Benchmarks derived from TPC-H. No comparison with official TPC results
is made or implied; no TPC compliance claim is made. Engine versions,
configurations, and host capacity are recorded per run, and all
deviations from the TPC-H Specification (schema layout, query text, run
methodology, metrics) are declared in this repository and in each result
record. See [NOTICE](NOTICE).

## Running

See [`ndc/README.md`](ndc/README.md) for the full stage driver and the
three profiles: `lite` (smoke, not citable), `full` (release discipline),
`comet-default` (the shipped default kit ending in a generated family
report).

```sh
./run.sh bootstrap 1 sf1        # new workspace for a scale point
cd $WS_ROOT/tpch-sf1
./run.sh build-scale            # dbgen → flat → nested → depths → formats
./run.sh size-check             # assert row counts vs ndc/sizes.csv
./run.sh comet-default          # default kit, cold-cache, parity gate, report
```

## Structure

```text
ndc/
  run.sh            stage driver: build, profiles, size-check, report
  spark_poc.py      runner: timing, parity, native/fallback accounting, JSON records
  monitor.py        sidecar /proc sampler (tree CPU, peak RSS, IO bytes)
  write_fmt.py      writes the nested table sets to Iceberg / Delta
  report.py         per family × format verdict report
  prepare sql       conv.sql, nested.sql, depths.sql (transforms) · parity.sql,
                    ext_parity.sql (DuckDB verification suites)
  queries/          24 .sql files + manifests (qgen convention)
  answers/          pinned per-query answers per scale factor
  bench.conf        cluster (D11) and storage (D12) targets
  sizes.csv         recorded dbgen row counts per scale factor
```

## Results so far

First campaign executed on a single 28-core host with local NVMe storage:
sf 0.5, sf 1, sf 10 — Parquet / Iceberg / Delta × vanilla / Comet, all
cells parity-green. The stable
findings: Comet's advantage concentrates in native scan+aggregate and
compounds with volume; the nested-manipulation family is its weak spot at
any volume; fallback-flattened cells hide native-execution effects. Raw
result records and the analysis live in the coordinating repository.
