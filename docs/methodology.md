# Measurement and publication rules

NDC is derived from TPC-H and inspired by TPC-DS. Its layouts, query text,
qualification scale, supported operations, timing, and metrics differ from both.
Results are not comparable to published TPC-H/TPC-DS results; no official TPC
primary or optional performance metric is calculated.

## Timing

Read latency starts immediately before `spark.sql(...).collect()` and ends after
all results reach the driver. It includes planning and materialization. Write
latency ends when the write/maintenance action returns after commit; reopening and
validation are outside its timer. Source scans are included in fresh writes.
Input preparation, reference computation, JVM startup, and declared warm-ups are
excluded. Every warm-up and every measured repetition must validate.

Queries use recorded seeded permutations for every repetition and stream. Engine
order alternates across format cells. Separate-process engine cells still have
order effects; this is not per-repetition interleaving across engines. Use repeated
campaigns with reversed `ENGINES` order for performance claims. `STREAMS>1` runs
independent seeded read streams through one Spark application; this measures a
shared-session workload, not independent client/server connections. Stream elapsed
is first submission to last completion, including dispatch gaps. Per-query resource
attribution is unavailable for overlapping streams.

Cache modes:

- `uncontrolled`: no claim about data residency; default for qualification/CI.
- `warm`: at least one validated warm-up is required. Intended working-set reuse;
  does not prove every filesystem block remains resident.
- `cold`: `sync` and the Linux page-cache drop must succeed before each sample.
  This describes OS page-cache treatment, not fresh JVM/JIT or every storage cache.
- `drop-failed`: diagnostic failure, never a comparable cold sample.

Cold runs are explicit (`CACHE=cold` or `DROP_CACHES=yes`) and require appropriate
passwordless sudo. They are intended for an isolated benchmark host. CI does not
request host-wide cache drops. Warm, uncontrolled and failed-drop samples cannot be
mixed into a comparison.

## Validity

Reference SQL is explicit for each workload. At sf0.0083, TPC-H-derived answers are
pinned and independently checked with DuckDB. At other scales, flat/nested parity
is a differential check; do not call it independent qualification. Run the tiny
qualification on each candidate engine before a larger performance campaign.
Synthetic full reads compare against independently generated Python records;
synthetic compute uses relational reference SQL. Write round trips validate source
results and committed output. Full-read and write-oracle outputs are collected in
driver memory; large output datasets need distributed canonical bag validation,
which is not implemented. Keep synthetic output sizes within the driver budget. Maintenance uses deterministic expected state.

No check defaults to success when its reference is missing. Compare complete typed
rows, nulls, duplicate multiplicity, and declared ordering. Decimal precision is
preserved. Runtime exceptions, wrong answers, incomplete repetitions, incompatible
campaigns, and missing engine counterparts cannot produce a misleading speedup.
Unsupported capabilities remain visible. Historical schema-v1 records and earlier schema-v2 records without an embedded
manifest are rejected by the current reporter; retain their original source and reports.

## Disclosure and comparisons

Schema-v2 cells include the complete manifest and its verified identity, campaign/dataset
identities, source hashes, source
commit when available, runtime versions, JAR hashes, selected effective settings,
host/CPU/memory information, run order seed, cache treatment, and per-sample status.
Dataset identity includes physical input file hashes and sizes. `dataset.json`
provides that inventory. Retain physical file layout and writer versions with it.
The dedicated `nix/` flake keeps dataset/result trees out of Nix source copies.
Spark and DuckDB are pinned; format preparation is shared by both engines.

Use matched formats for layout effects, a fixed format/layout for engine effects,
and a fixed engine/layout for format effects. `mixed` explicitly keeps flat
controls as Parquet. Format differences still include writer layout, file sizes,
compression, metadata, and reader behavior; do not attribute the entire result to
one factor without a controlled experiment. Physical plans and per-operator
metrics accompany samples to inspect projection, pruning, bytes, rows, shuffle,
and spill where the engine exposes them. Missing counters are not invented zeros. Write file/byte inventories include
non-hidden data and metadata files; `new_file_bytes` counts newly created file
paths, not overwritten bytes or total device traffic. Logical changed-row rates
are separate from full materialization output-row rates.

Comet native operator fraction counts plan nodes with Comet names. It is **not a
fraction of elapsed time, rows, or bytes**, and other plan nodes are not automatically
fallback defects. Write records currently capture their verification-read plan;
that plan cannot support claims about native write execution.

The Linux sampler accumulates per-process CPU/IO deltas across child lifetimes and
sums current tree RSS. Shared mappings may be counted more than once; short-lived
process work between samples can be missed. Short/unbracketed windows are
unavailable, not zero. Boundary slop and sample count accompany usable windows.
Sampled peak RSS is not an exact process high-water mark.

Reports retain per-query medians, sample counts and ranges. Family totals are
explicit descriptive sums; no aggregate score weights eight Q6 variants as eight
independent business workloads. Three samples provide exploratory medians, not
statistical evidence of small improvements. Use additional repetitions and repeat
campaigns when making performance claims.

## Result artifacts

Keep the entire campaign directory: cell JSON, plans, stdout, monitor samples,
write targets when needed for inspection, report, and `campaign.json`. A checksum
bundle should also include the source snapshot, artifact lock, dataset inventory,
configuration and commands. `run.sh bundle <campaign>` creates a portable evidence
archive plus SHA-256 sidecar. Bundling rejects source bytes or dataset/format
inventories that no longer match the measured cells. Publishing is separate from generation; attach a
bundle to a release or durable artifact store, then link its immutable location
from any claim. GitHub CI uploads qualification diagnostics for 14 days; those
expiring artifacts are not a durable public benchmark archive.
