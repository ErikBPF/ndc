# Shared dataset preparation

This standalone tool imports source tables, constructs the lossless nested model
and writes canonical Parquet. DuckDB is a preparation dependency; consuming engines
do not need it. Python and DuckDB are supplied by the repository's locked Nix shell.

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
(column order may differ). Decimal values must fit
`DECIMAL(15,2)` exactly. Text input uses integer keys and ISO `YYYY-MM-DD` dates;
invalid values are rejected instead of rounded.

The transformer uses four worker threads and a `4GB` DuckDB memory limit by default.
Set `--memory-limit 16GiB` or `NDC_PREP_MEMORY=16GiB` for a larger preparation budget;
the CLI option takes precedence. The manifest records preparation settings separately
from dataset identity. These are engine settings, not process-level resource caps.
Allow disk for the source, working database and output. Generation writes into a fresh directory. Failed
outputs remain for diagnostics with a failed manifest; choose a new output path
for a retry. A successful manifest is written only after validation and checksums.

The memory override is a resource control, not a guarantee that a particular
scale will fit.

## Consume and qualify

Use [engine DDL and query templates](../engines/README.md). The `.tbl` format is an
input interface, not an engine-specific generator implementation. Engines consume
the same Parquet artifacts; loading into a different physical format must retain
all logical values and be disclosed with measurements.

`./ndc/run.sh check` exercises import, numeric precision, keys, ordering, empty
parents, artifact verification and DuckDB query semantics. CI also runs
[the Spark cross-engine test](../tests/portable_spark.py). Snowflake execution needs
its own live qualification; publishing a DDL file is not evidence of a passing run.
