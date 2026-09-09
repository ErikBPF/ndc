# Workload contracts

NDC combines **TPC-H-derived entities and queries**, **TPC-DS-inspired analytical
and execution patterns**, and explicitly separate synthetic nested-shape data.
It is neither a complete TPC-H nor a complete TPC-DS implementation.

Sources: [TPC-H](https://www.tpc.org/tpch/default5.asp),
[TPC-DS 4.0.0, clauses 3, 5, 7 and Appendix B](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v4.0.0.pdf).
TPC-DS Q67 motivates category ranking; Q80 motivates combining sales/returns
channels. These are conceptual adaptations with new SQL and synthetic inputs;
NDC does not generate the DS schema or execute its 99-query suite.

Workload IDs, logical data, expected results and ordering/null semantics define
the benchmark across engines. SQL files and storage operations in this repository
are the bundled Spark implementation; another engine may translate them while
preserving those contracts. See [engine integration](engines.md).

## Data contracts

The [shared-data contract](data-contract.md) retains all order and line-item
fields and is used by the measurement suites.

- TPC-H: DuckDB's `tpch` extension supplies eight base tables. `sizes.csv` records
  exact counts for 0.0083, 0.5, 1 and 10, including nation and region.
- `orders_nested_v2`: complete order headers plus ordered `array<struct>` line
  items, retaining empty parents. Preparation validates keys, relationships and
  exact header/leaf bags; `invariants` verifies the canonical manifest and file
  checksums. Leaf order is explicitly `l_linenumber`.
- `orders_depth1..8`: the same four-field Q6 leaf with alternating wrappers.
  Added arrays are singletons. This suite isolates wrapper overhead, not fan-out.
- `shape`: synthetic parents with nullable `items`, independent `returns`,
  branching `groups[].items[]`, parallel `amounts`/`tags` arrays, an attribute map,
  and unused padding fields. `shape_flat`, `shape_returns`, `shape_dim` are controls.
  `shape.json` records generation parameters; `shapes.records` is deterministic.

`PARENTS`, `FANOUT`, `WIDTH`, and `DATA_SEED` control the synthetic fixture at build
time. `DATA_SEED` defaults to 7; `QUERY_SEED` only controls query order.
Null arrays, empty arrays, null elements, null amount/tag leaves, duplicate values,
and ties are deliberate. Parent cardinalities cycle through 0, 1, 4, 16, and the
maximum fan-out (bounded by that maximum). Sweep fan-out and width in fresh
workspaces; retain equal parent counts/seed and disclose changed element counts.
Suggested small shape experiments: fan-out 1/16/64/1024 and width 1/8/32.
Selectivity cases use amount thresholds 0/1/50/100 on seeded values in [0,100).
Thresholds approximate the non-null selectivity; actual qualifying counts depend
on seed/scale and must not be presented as exact percentages.

## Query semantics

Manifest entries define `sql`, `reference`, `family`, `operation`, `layout`, and
`ordered`, plus an optional pinned `answer`, Python `oracle`, write `action`, and
inspiration. Report grouping uses metadata, not filename prefixes. SQL stays in
reviewable files. Changing semantics requires changing/reviewing reference answers.

- E13 is a parent-column pruning control; it never reads the child array.
- E18 sums both of the two most expensive items per order. It reports the number
  of qualifying orders and their total; it cannot reduce to a maximum predicate.
- Q6 validates revenue **and count**, including all depth variants. Empty-match
  revenue is NULL and count is zero. E19 also preserves NULL SUM semantics.
- `s_full` materializes the complete selected nested payload. `VALIDATION=collect`
  brings it to the driver; `VALIDATION=distributed` persists it on executor disk.
  Both include serialization and materialization, so neither measures pure disk
  bandwidth. See [validation modes and memory limits](methodology.md#distributed-validation).
- `s_regroup` reconstructs selected child structs; `s_topn` depends on two values.
- `ds_rank` joins a dimension, aggregates by category, then ranks.
- `ds_channels` unions positive sales and negative returns, then rolls up channel
  and category. It avoids an accidental cross-product of independent child arrays.

Null arrays and empty arrays remain distinct in full reads/writes. Each compute
query explicitly chooses its null handling. Expected bags preserve duplicates.
Array order matters even when outer result-row order is declared immaterial.
Integers/decimals compare exactly; there is no global rounding or float tolerance.

## Writes and maintenance

Every sample uses a new target; state setup and expected-answer construction occur
before timing. Materialize/transform/construct time source execution and committed
output. Append adds one fresh parent with an empty items array. Update increments
parent 1's nested `payload.amount`; delete removes parent 1. Compaction preserves
all logical rows. The tiny append is a transaction-latency probe, not a bulk ingest
throughput claim; materialization supplies the bulk-write case.

In the bundled Spark runner, Iceberg uses table operations and `rewrite_data_files`; Delta uses table writes,
UPDATE/DELETE and OPTIMIZE. Parquet supports fresh writes, append, and compaction
into a new materialized path, but not transactional UPDATE/DELETE. Compaction of
already compact data may do no work; disclose before/after storage and file counts.
NDC does not silently replace unsupported transactions with a full-table rewrite.

Single-stream query latency, concurrent read streams, and maintenance are distinct
experiments. The bundled runner rejects cold-cache concurrent streams and concurrent
maintenance. Other runners must declare their supported execution combinations.
