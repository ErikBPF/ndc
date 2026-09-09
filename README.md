# NDC — Nested Data Compute

**Nested Data Compute (NDC)** is an engine-agnostic benchmark for scanning,
materialized reads, nested computation, writes, and maintenance. Workload semantics
and correctness contracts apply across engines. Implementations use their own SQL
dialect and execution model. Its relational baseline and historical `ndc-tpch` suite
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

## Getting started

1. Choose workloads from the [data and query contracts](docs/workloads.md).
2. Select an implementation from [engine scripts](docs/engines.md#engine-scripts)
   for setup, supported capabilities and execution commands.
3. Qualify correctness before measurement, then publish results with the
   [required disclosure and evidence](docs/methodology.md).

To support another engine, follow the [implementation contract](docs/engines.md).
SQL dialects, storage formats and execution models may differ; workload semantics
and correctness requirements remain shared.

### Using a coding agent

Copy and paste the [agent usage prompt](docs/agent-usage.md) into your agent to
guide setup, correctness qualification, measurement and reporting.

## Shared data across engines

The [data-model one-pager](docs/data-contract.md) defines a lossless nested schema,
[standalone preparation](datagen/README.md) and [dialect-specific DDL/query templates](engines/README.md).
The runner and engine adapters use this same canonical dataset contract.

## Validity before comparison

Every timed repetition is checked, including complete rows, duplicate multiplicity,
nulls, and declared ordering. The tiny TPC-H suite checks pinned answers against
independent flat reference SQL. Larger scales use
explicit flat/nested reference comparisons. Synthetic full reads have a separate
oracle; other synthetic queries use equivalent reference SQL. Writes reopen
committed output and compare complete contents; maintenance checks expected state.

Failed, missing, unvalidated, stale, or incompatible results cannot receive a
speedup comparison. Raw failure records remain available. Reports show per-query
medians, sample counts/ranges, and descriptive family totals, without an overall
weighted score or an automatic significance verdict.

## Documentation

- [Benchmark contract and engine scripts](docs/engines.md)
- [Workloads and schemas](docs/workloads.md)
- [Measurement and validity rules](docs/methodology.md)
- [Candidate comparisons and experiments](docs/experiments.md)
- [TPC-H and TPC-DS alignment](docs/tpc-alignment.md)
- [Test interface compared with TPC-DS](docs/test-interface.md)
- [Answer provenance](ndc/answers/README.md)
- [Validation and CI](docs/validation.md)
- [Attribution and deviations](NOTICE)

Performance claims must identify the measured implementation and link a complete
immutable artifact bundle, including source identity, raw samples, plans and
checksums. Results from another harness revision require qualification under that
revision’s measurement and validity rules.

NDC also stands for “Ninho de Cobra,” a nickname for the benchmark for my Brazilian friends.
