# NDC test interface and TPC-DS

NDC has analogous workflow concepts, but its CLI and execution contract are not
TPC-DS-compatible. Here, “interface” means commands, configuration, generated
workloads, phase sequencing, stream behavior and result artifacts.

## Current mapping

| Concern | TPC-DS | NDC today |
|---|---|---|
| Data preparation | `dsdgen`; generation and database load | `bootstrap` + `build-scale`: DuckDB TPC-H generation, exports, nested layouts, synthetic shapes and format copies |
| Query preparation | `dsqgen`; 99 parameterized templates | Explicit manifests and fixed SQL; `SEED` changes permutations and synthetic data, not query parameters |
| Serial measurement | Power Test | `matrix` with `STREAMS=1`; selected read/write workloads and repeated samples |
| Concurrent measurement | One session per stream | `throughput`: threads share one Spark session; default two streams of the two DS-inspired queries |
| Maintenance | Generated refresh sets | Selectable update/delete/compaction workloads on fresh per-sample targets |
| Qualification | Reference-answer validation | `qualify` checks 24 tiny pins through DuckDB; candidate-engine validation happens during `matrix`/`spark` |
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

## Recommended changes, in priority order

1. **Make suite and phase names unambiguous.** The root suite table calls `full`
   the historical 24-query membership, while `run.sh full` selects all 46 workloads.
   Expose a clear suite selector and distinguish serial read measurement from an
   engine/format matrix. Preserve existing commands as documented compatibility
   aliases if the interface changes.
2. **Separate harness checks from benchmark qualification.** `test` currently runs
   Python and shell checks; `qualify` checks reference pins, not a selected engine.
   A candidate qualification command should combine sizing, structural checks,
   reference pins and engine execution, with one final pass/fail record.
3. **Freeze the execution schedule before running.** Retain the expanded workload,
   query order, stream IDs, repetitions and any parameter bindings as an artifact.
   Existing manifests already retain SQL and references in results. Split data and
   query seeds when their variation becomes independently selectable; keep fixed
   selectivity cases fixed unless a parameter sweep is explicitly requested.
4. **Keep shared-session throughput clearly named.** Introduce separate sessions or
   clients only when measuring independent-client concurrency is a goal. Do not
   imply that adding threads alone implements the TPC-DS stream contract.
5. **Add an optional phase plan only if needed.** A plan could orchestrate load,
   serial reads, concurrent reads and maintenance using existing primitives. Do
   not rename isolated scratch-table mutations as database refresh runs or add an
   official-looking aggregate score.

These are recommendations, not implemented CLI commands. The immediate priority
is naming and qualification semantics; a new driver framework is unnecessary.

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
