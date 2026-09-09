# Shared data model

Nested Data Compute separates the dataset, workload semantics and engine execution.
Generate one canonical dataset and let each engine load it through its own DDL and
SQL dialect. An engine must not independently regenerate different input for a
comparison. NDC remains TPC-H-derived and TPC-DS-inspired, without TPC compliance
or a TPC performance score.

## Canonical dataset: version 2

The [schema contract](../datagen/schema.json) defines eight relational TPC-H tables
and `orders_nested_v2`. The nested table contains every order field and an ordered
list of line-item records. Each child retains every line-item field except
`l_orderkey`, which is reconstructed from its parent’s `o_orderkey`.

Keys use integers, financial and quantity fields use exact `DECIMAL(15,2)`, dates
use `DATE`, and text retains its values. These are NDC's physical type choices for
representing the source data, not a claim to reproduce every generator's DDL.

- One nested row per order; each source child appears under its original parent.
- Children are ordered by `l_linenumber`; parent and child keys are unique.
- Parents without children receive an empty list, never a fabricated null child.
- Baseline fields, lists and child records are non-null. Separate query fixtures
  exercise null lists, empty lists, null elements and duplicate elements.
- Exact bidirectional bag checks reconstruct all order and line-item fields.

## Generate once, consume many times

The [standalone importer and transformer](../datagen/README.md) accepts the eight
unpartitioned `.tbl` files from a pinned TPC-H generator, or eight typed Parquet
source tables (single files or Parquet directories). DuckDB and Spark preparation
backends implement the same logical model, independently of the consuming engine.
The official generator is obtained and run separately.

Output contains nine Parquet tables and `dataset.json`: schema identity, input
checksums, declared source label, writer version, row counts and output checksums.
Tables may be single files or directories of Parquet parts. A table's `sha256`
evidence is a digest for a single file, or a filename-to-digest map for a directory.
Artifact identity includes that physical layout; schema identity is unchanged.
Invalid keys, orphan children, lossy typed casts and malformed numeric text fail
before a successful dataset is declared. Existing output directories are refused.
`--verify` checks the schema and artifact identities before import; it does not
replace semantic validation inside the consuming engine.

Every engine receives the same canonical files. Internal storage may differ after
loading; disclose those differences separately from input identity. Preparation
and loading are separate from timed query execution.

## DDL and workload adapters

[DuckDB](../engines/duckdb/ddl.sql), [Spark](../engines/spark/ddl.sql) and
[Snowflake](../engines/snowflake/ddl.sql) DDL render the same nested schema.
The [portable workload contract](../datagen/workloads.json) defines inner line-item
aggregation and parent-preserving outer counts. SQL templates translate those
operations into each dialect; shared semantics contain no engine SQL text.

DuckDB and Spark qualification checks load the same fixture, preserve complete
values and validate the query answers. Snowflake templates require live
qualification against the chosen table/loading mode before measurements.

## Runner integration and next boundary

The benchmark runner uses this same lossless dataset and `orders_nested_v2`
table. Depth and shape fixtures extend it for their workloads. `dataset.json`
retains the canonical contract; `inventory.json` identifies all benchmark files,
including those fixtures. There is no separate reduced nesting projection.

Portable SQL examples do not imply that every workload or result reporter has
been ported to every engine. Extend engine qualification and semantic result
identity before making cross-engine performance comparisons. See
[adapter usage](../engines/README.md).
