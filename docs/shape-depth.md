# Controlled shape and depth experiments

This synthetic family complements the TPC-H-derived dataset. It is not a TPC
scale factor or a replacement for the legacy shape fixtures. The
[workload contract](../datagen/shape-workloads.json) defines four operations at
array depths 1, 3 and 5, plus flat controls:

| Operation | Work measured |
|---|---|
| Filter/aggregate | Flatten, select amounts >= 50, count and sum |
| Join/aggregate | Flatten, join parent attributes, aggregate by bucket |
| Top-N | Flatten, rank within each parent, sum the two largest non-null amounts |
| Transform/construct | Flatten, filter, double amounts, retain tag/padding, reconstruct ordered arrays |

Transform/construct materializes a query result; it does not measure a committed
storage write. Existing write and maintenance workloads keep that responsibility.
Each depth preserves the full leaf bag and parent membership. Null amounts, empty
parents and null parent arrays have explicit semantics. Null child records and
transactional deep updates remain coverage of other fixtures or future extensions.

## Separate depth, branching and population

Preparation writes one shared Parquet dataset with flat leaves, parent attributes,
and all three depth tables. Engines consume these files rather than generating
independent inputs. DuckDB is used only as the preparation writer.

`--layout singleton` adds single-child wrappers. `--layout branching` partitions
the same ordered leaves among up to `--branching` children at every added level.
The flat leaf fields are `leaf_id BIGINT`, nullable `amount BIGINT`, `tag VARCHAR`
and `padding VARCHAR`, with a `parent_id BIGINT` foreign key. The parent table has
`parent_id BIGINT` and `bucket BIGINT`. Depth tables have `parent_id` and `children`:
a list of leaf structs at depth 1, or structs containing another `children` list
at each added level. All non-amount leaf fields are non-null.

Neither layout duplicates leaves to create depth. Struct and array depths count
containers along the children-to-leaf path, excluding the table row; both equal
the selected depth. A depth-1 leaf is already a struct inside an array.

Choose one population mode:

- **Fixed parents:** `--parents N`. Each parent has 0 through `--fanout` leaves.
  A leaf's identity is `(parent_id << 32) + child_position`. Overlapping positions
  keep their values when fan-out or shape seed changes; total volume can change.
- **Fixed leaves:** `--leaves N`. The population is exactly IDs 0 through N-1.
  Fan-out and shape seed change their grouping into parents, not their values.
  Parent count can change. This mode creates no empty parents; fixed-parent mode
  supplies those cases. Zero leaves produces typed empty tables.

The generator records parent/leaf counts, the parent-cardinality histogram,
array-length histograms, declared depths and each table's compressed bytes. Use
fixed-leaf mode to study structure with equal logical population; use fixed-parent
mode to study growth. Neither guarantees equal physical bytes or file layout.

## Seeds and distributions

`--data-seed` and `--shape-seed` default independently to 7. This tool does not
inherit the legacy `SEED` environment variable.

Values use a versioned SHA-256 mapping of seed, leaf identity and field. There is
no sequential RNG whose call count shifts later values. Shape seed controls
parent cardinality and leaf order. Changing data seed preserves shape membership.
`QUERY_SEED` belongs to the measurement runner and never changes the dataset.

`--distribution uniform` draws parent counts across 0..fanout. `skewed` makes
roughly one tenth maximum-sized and the others small; sample counts are measured,
not promised percentages. Fixed-leaf mode clamps each group to at least one leaf
and truncates the last group to preserve the requested total.

`--null-rate` controls nullable amounts, not array cardinality or null containers.
Empty parents deterministically receive either null or empty arrays. `--width`
sets padding characters per leaf. `--entropy low` repeats one character; `high`
uses deterministic SHA-256 hex blocks. Transform/construct reads and materializes padding; the other queries provide
column-pruning controls. These are compressibility controls, not
claims of uniformly random bytes. Padding retains its prefix when width changes.

## Prepare and verify

From the repository root:

```sh
devenv --profile duckdb shell -- python3 datagen/shapes.py \
  --out workspaces/shapes-example/data --leaves 10000 --fanout 64 \
  --data-seed 7 --shape-seed 7 --layout branching

devenv --profile duckdb shell -- python3 datagen/shapes.py \
  --verify workspaces/shapes-example/data
```

Use a fresh output directory for every configuration. Failed generation leaves a
failed contract for diagnostics. Verification checks contract identity and all
recorded table/oracle checksums. It is artifact verification, not a substitute for
executing the engine's qualification checks.

`shape-contract.json` records generator version/source identity, all data/shape
parameters, logical leaf-population identity, artifact hashes and writer version.
Independent Python answers are streamed to JSONL files. Preparation holds one
parent's tree in memory and stages JSONL on disk; budget memory for maximum fan-out
and width, and disk for staging plus Parquet. DuckDB uses four threads and a 4 GB
engine memory limit; process limits are configured separately.

Use [engine scripts](../engines/README.md#shape-depth-workloads) for SQL and timed
execution. Candidate comparisons must use the same dataset within each sweep cell.
Do not pool different seeds or shapes as repeated samples of one dataset. The
contract lists CI seeds 7/19 and performance seeds 7/19/43; repeat each performance
cell independently. Frozen query schedules and balanced candidate order remain the
existing runner's responsibility.

## Qualification and compatibility

DuckDB regression tests compare all depth queries with independent Python answers.
The Spark qualification script also checks null/empty parent arrays, full
bidirectional leaf bags and the
same answers for both layouts and multiple data seeds. CI runs these small cases
and a public-runner test that checks the physical Iceberg scan in mixed mode.
The legacy sequential-RNG fixtures remain unchanged, so old expected answers and
measurements retain their meaning. New experiments should use this family when
stable identities across shape sweeps are required.
