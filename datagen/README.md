# Shared dataset preparation

This standalone tool imports source tables, constructs the lossless nested model
and writes canonical Parquet with DuckDB or Spark. Consuming engines do not need
the preparation backend. Python and DuckDB come from the locked Nix shell;
`./ndc/run.sh setup` installs the pinned Spark runtime.

Read the [data-model one-pager](../docs/data-contract.md) and
[schema](schema.json) before choosing an engine loader.

## Import generator output

Build/run the official TPC-H generator separately, pinning its source revision,
scale factor and generation options. Put its eight unpartitioned `.tbl` files in
one directory. The importer expects the standard trailing `|` delimiter and does
not interpret CSV quoting. Sharded generator output must be assembled into those
eight files before import.

From the NDC repository root:

```sh
nix develop path:nix -c python3 datagen/generate.py \
  --input /path/to/dbgen-output \
  --out workspaces/shared/sf1-v2 \
  --source-label 'TPC-H dbgen <revision>; SF1'

nix develop path:nix -c python3 datagen/generate.py \
  --verify workspaces/shared/sf1-v2
```

`--source-label` is a declaration, not proof of the generator revision or scale.
Input file checksums identify the actual bytes. Preserve the generator source,
configuration and original files with published evidence. This importer validates
schema, relationships and round trips; it does not certify TPC scale cardinalities.

Existing flat Parquet exports can also be imported:

```sh
nix develop path:nix -c python3 datagen/generate.py \
  --input /path/to/flat-parquet --input-format parquet \
  --out workspaces/shared/converted-v2 --source-label 'source dataset identity'
```

Expected names: `region`, `nation`, `supplier`, `customer`, `part`, `partsupp`,
`orders`, `lineitem`, with the selected file extension. Parquet columns must match the schema exactly
(column order may differ). Schemas are checked across every Parquet part, so an
extra column in a later part is rejected. Decimal values must fit
`DECIMAL(15,2)` exactly. Text input uses integer keys and ISO `YYYY-MM-DD` dates;
invalid values are rejected instead of rounded.

The default DuckDB backend uses four worker threads and a `4GB` DuckDB memory limit by default.
Set `--memory-limit 16GiB` or `NDC_PREP_MEMORY=16GiB` for a larger preparation budget;
the CLI option takes precedence. The manifest records preparation settings separately
from dataset identity. These are engine settings, not process-level resource caps.
Allow disk for the source, working database and output. Generation writes into a fresh directory. Failed
outputs remain for diagnostics with a failed manifest; choose a new output path
for a retry. A successful manifest is written only after validation and checksums.

DuckDB nests order-key ranges independently (`--key-span 250000` by default),
then flushes each range to `orders_nested_v2.parquet/part-*.parquet`. Flat tables
remain individual files. Disk-backed validation checks the complete result.
Smaller ranges reduce aggregate memory at the cost of more scans and files.
A single parent's children must still fit in memory. Empty tables and orders
without children retain the same schema and empty-array semantics.

## Spark preparation

```sh
./ndc/run.sh setup
nix develop path:nix -c "$HOME/ndc-spark41/spark-4.1.3-bin-hadoop3/bin/spark-submit" \
  --master 'local[4]' --deploy-mode client --driver-memory 4g \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.local.dir=/path/to/disk/scratch \
  datagen/generate.py --backend spark \
  --input /path/to/flat-parquet --input-format parquet \
  --out workspaces/shared/spark-v2 --source-label 'source dataset identity'
```

Spark also accepts `--input-format tbl`. Configure driver/executor memory, cores,
master and shuffle partitions through `spark-submit`; `--memory-limit` and
`--key-span` configure DuckDB only. Spark uses sort aggregation, preserving ordered
children and empty parents, and writes each table as a directory of Parquet parts.
It does not collect source rows on the driver or cache the full dataset.
Use client deploy mode so the driver has this checkout and its schema file.
Input and output paths must be visible at the same absolute location to the driver
and every executor (for example, a shared filesystem). Object-store paths and
cluster provisioning are not implemented by this CLI.

Both backends reject invalid source keys and values and validate exact order and
line-item round trips before publishing success. `--verify` checks each table file or collection; checksums cover every part.
File names, compression and partitioning can differ between writers, so artifact
IDs can differ for logically equal data. Prepare once and share those exact files
for benchmark comparisons.

Describe Spark sizing with driver CPU and memory, executor count, and CPU and
memory per executor. For example, a configurable profile might use a 1 CPU /
2 GiB driver and two 4 CPU / 16 GiB executors. These are example container
resources, not validated sizing for a particular scale. Configure JVM heap and
memory overhead within each container's memory budget. Record CPU requests and
limits separately when they differ, and provision disk-backed scratch storage.

## Consume and qualify

Use [engine DDL and query templates](../engines/README.md). The `.tbl` format is an
input interface, not an engine-specific generator implementation. Engines consume
the same Parquet artifacts; loading into a different physical format must retain
all logical values and be disclosed with measurements.

`./ndc/run.sh check` exercises import, numeric precision, keys, ordering, empty
parents, artifact verification and DuckDB query semantics. CI also runs
[the Spark cross-engine test](../tests/portable_spark.py), both with the default
DuckDB fixture and with `--spark-backend`. Snowflake execution needs
its own live qualification; publishing a DDL file is not evidence of a passing run.
