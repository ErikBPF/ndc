# NDC test interface and TPC-DS

NDC has analogous workflow concepts, but its CLI and execution contract are not
TPC-DS-compatible. Here, “interface” means commands, configuration, generated
workloads, phase sequencing, stream behavior and result artifacts.

## Current mapping

| Concern | TPC-DS | NDC today |
|---|---|---|
| Data preparation | `dsdgen`; generation and database load | `bootstrap` + `build-scale`: DuckDB TPC-H generation, exports, nested layouts, synthetic shapes and format copies |
| Query preparation | `dsqgen`; 99 parameterized templates | Fixed SQL in explicit manifests; independent `DATA_SEED` and `QUERY_SEED`; SQL and expanded schedules retained in `plan.json` |
| Serial measurement | Power Test | `latency` selects serial reads; `matrix` separately selects engine/format cells |
| Concurrent measurement | One session per stream | `shared-throughput`: threads share one Spark session; default two streams of the two DS-inspired queries |
| Maintenance | Generated refresh sets | `maintenance`: update/delete/compaction workloads on fresh per-sample targets |
| Qualification | Reference-answer validation | `qualify-references` checks pins; `qualify-engine` combines data gates and all candidate workloads |
| Reporting | Specified performance metrics and disclosure | Per-query medians, ranges, descriptive totals, source/data identities and evidence bundles; no TPC metric |

TPC-DS prescribes Load → Power → Throughput 1 → Maintenance 1 → Throughput 2 →
Maintenance 2. NDC has no command enforcing that sequence. These TPC-DS references
come from [the 4.0.0 specification, clauses 3–5 and 7](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v4.0.0.pdf).

NDC's engine/format matrix, nested-shape controls and executor-side validation are
purposeful benchmark features. They should remain explicit rather than being
presented as official TPC phases. Distributed-mode read timing includes executor
materialization; throughput elapsed time also includes validation gaps between
queries. These measurements serve different purposes from a protocol-compatible
TPC-DS driver.

## Implemented interface improvements

- **Explicit membership:** `SUITE=tpch` selects 24 historical queries; `SUITE=read`
  selects 39 read/compute cases; `SUITE=all` selects 46 workloads. Custom `QUERIES`
  overrides suite selection. Existing commands remain compatibility aliases.
- **Separate phases:** `check`, `qualify-references`, `qualify-engine`, `latency`,
  `shared-throughput`, `maintenance`, and `matrix` identify their actual work.
  Candidate qualification emits one final verdict after sizing, structural checks,
  reference pins, parity and engine execution; failed gates stop the sequence.
- **Frozen schedules:** `plan.json` is written before Spark starts. It retains full
  SQL, references, query order, stream IDs, repetitions and warm-ups. Results embed
  the plan identity; reports reject order or coverage drift. `plan <out.json>`
  previews a matrix schedule without execution.
- **Independent seeds:** `DATA_SEED` controls synthetic generation; `QUERY_SEED`
  controls permutations. `SEED` remains the fallback. Fixed selectivity cases remain
  fixed; there is no implicit parameter sweep.

The shared-session model remains explicit; separate clients are unnecessary for
this nested-engine comparison. There is no new multi-phase driver framework or
TPC-DS refresh protocol. The phase commands compose from the existing primitives,
and no official-looking aggregate score is introduced.

## Test disclosure

Describe allocated resources, not machine names or total machine capacity. The
retained six-cell qualification used a 400% CPU quota, a 16 GiB scope memory cap,
`local[4]` execution threads, an 8 GiB driver heap, and a configured 2 GiB Comet
off-heap pool. The quota does not imply four dedicated or pinned cores. Report
validation mode, dataset size, streams, warm-ups and repetitions alongside these
limits. The focused Spark validator used `local[2]`, a 2 GiB driver heap and a
1 MiB driver result-size limit within the same scope limits.

See [retained validation evidence](validation-2026-09-07.md),
[commands](../ndc/README.md), and [measurement rules](methodology.md).
