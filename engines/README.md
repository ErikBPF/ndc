# Shared-data SQL adapters

These DDL and query templates implement the [version 2 data contract](../docs/data-contract.md).
They are starter adapters, not replacements for the complete
[benchmark runner](../docs/engines.md#engine-scripts).

| Engine | DDL | Queries | Qualification |
|---|---|---|---|
| DuckDB | [DDL](duckdb/ddl.sql) | [Inner](duckdb/queries/lineitem_totals.sql), [outer](duckdb/queries/outer_item_counts.sql) | Repository regression tests |
| Spark | [DDL](spark/ddl.sql) | [Inner](spark/queries/lineitem_totals.sql), [outer](spark/queries/outer_item_counts.sql) | `tests/portable_spark.py` |
| Snowflake | [DDL](snowflake/ddl.sql) | [Inner](snowflake/queries/lineitem_totals.sql), [outer](snowflake/queries/outer_item_counts.sql) | Live qualification required |

DDL is generated from `datagen/schema.json`; regenerate a dialect with
`python3 datagen/generate.py --ddl duckdb` (or `spark`, `snowflake`). Tests check
that published DDL matches the schema. Nested nullability and key constraints are
also checked by the generator; engines need not enforce every constraint in DDL.

Verify the canonical dataset before loading. For DuckDB, after preparing the
example dataset in the generator guide:

```sh
devenv --profile duckdb shell -- duckdb workspaces/shared/duckdb.duckdb < engines/duckdb/ddl.sql
devenv --profile duckdb shell -- duckdb workspaces/shared/duckdb.duckdb -c \
  "INSERT INTO orders_nested_v2 SELECT * FROM read_parquet('workspaces/shared/sf1-v2/orders_nested_v2.parquet/*.parquet');"
devenv --profile duckdb shell -- duckdb workspaces/shared/duckdb.duckdb < engines/duckdb/queries/lineitem_totals.sql
```

For Spark, execute its DDL in an isolated warehouse, then load the same file or directory with
`spark.read.parquet(path).write.insertInto('orders_nested_v2')`. Qualification also
flattens every child and checks both directions against the original source rows.

Snowflake uses typed ARRAY/OBJECT fields, with quoted lowercase child names.
Choose a supported table type and load/cast the canonical Parquet fields into
that DDL. Configure staging and credentials outside this repository. These files
do not provision a warehouse or implement a loading pipeline. See the official
[structured-type documentation](https://docs.snowflake.com/en/sql-reference/data-types-structured).

The [workload contract](../datagen/workloads.json) is shared across dialects. Keep
its semantic identity separate from SQL text and execution settings. The legacy
reporter requires SQL-bearing manifest identity and cannot establish comparability
between these translated queries.
