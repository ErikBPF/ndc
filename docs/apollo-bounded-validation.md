# Bounded preparation: Apollo validation, 2026-09-08

Only SF0.5 and SF10 were exercised on Apollo, plus tiny contract fixtures.
The initial NFS campaign is recorded first; the completed Spark SF10 ext4
follow-up is recorded below.
DuckDB used four threads, an 8 GiB engine memory limit and key spans of 250000.
Spark used local[4], a 4 GiB driver heap and 200 shuffle partitions. Inputs were
identical retained flat exports from the earlier Apollo run; data and scratch
were on NFS. These are correctness checks, not isolated timing comparisons.

| Preparation | Result | Seconds | Peak sampled process-tree RSS, GiB |
|---|---|---:|---:|
| duckdb-sf05 | PASS | 31.0 | 2.262 |
| spark-sf05 | PASS | 158.9 | 3.491 |
| duckdb-sf10 | PASS | 1895.0 | 8.766 |
| legacy-sf05 | PASS | 14.5 | 1.255 |
| legacy-sf10 | PASS | 508.4 | 8.720 |

Both legacy scales passed query parity. Both canonical DuckDB scales and Spark
SF0.5 passed in-generation input validation, exact order/line-item round trips
and output checksumming. SF0.5 also passed standalone artifact verification for
both backends; their source checksums, schema identity and row counts match.

The user requested stopping after the active run to reboot Apollo. DuckDB SF10
completed successfully; orchestration was stopped before Spark SF10 or a separate
SF10 verifier could start. All task processes were stopped before reboot clearance.
Spark SF10 subsequently completed on ext4 as recorded below. SF1000 and
Kubernetes execution were not tested.

Final review validation: 70 repository tests plus shell checks passed locally and
on Apollo. Spark fixtures at local[2] / 2 GiB passed ordered children, empty parents,
full round trips, inner/outer query parity, DuckDB reimport of Spark file collections,
and rejection of duplicate keys, orphan keys and excess decimal precision.
CI passed after fixing the second spark-submit invocation.

The scale-run snapshot preceded review additions for empty-input handling,
explicit checksummed file lists and batched Spark text validation. Those changes
were exercised by the final fixture/repository checks; scale inputs were nonempty
single Parquet files. Exact code fingerprints and structured measurements are in
[the evidence JSON](evidence/apollo-bounded-20260908.json). SF10 DuckDB duration is
approximated from the first and last live one-second resource samples because
orchestration was paused to prevent the queued run.

Engine memory limits are not process RSS caps. RSS samples sum descendants and
may count shared pages twice. Legacy and fixture checks overlapped some canonical
preparation. Logs and datasets remain on Apollo's NFS storage; evidence is also
retained locally.

## Spark SF10 on ext4: 2026-09-09 UTC

After reboot, NFS was unreachable. The follow-up used `/mnt/microvms` (ext4 on
`/dev/md127`) for source, output and Spark scratch. Pinned tools regenerated SF10;
all eight source-file checksums exactly match the earlier DuckDB SF10 inputs.
Schema identity and every table's row count also match the earlier result.

| Stage | Result | Seconds | Peak sampled process-tree RSS, GiB |
|---|---|---:|---:|
| Flat generation, DuckDB 8 GiB limit | PASS | 152.7 | 3.483 |
| Spark preparation, local[4], 4 GiB driver | PASS | 855.1 | 5.582 |
| Standalone artifact verification | PASS | 12.5 | 0.021 |

Spark preserved 15,000,000 orders and 59,986,052 line items, writing 200 nested
Parquet parts. Input validation, exact bidirectional order/line-item round trips,
output checksums and standalone verification all passed. Logs show external
sorting spilling to disk. The 4 GiB setting is the JVM heap, not total RSS.

The tested source is commit `815a7b94eaceea29fafb56d614e61a5552e12fe0`.
[Structured ext4 evidence](evidence/apollo-ext4-20260909.json) records commands,
settings, source identity, row counts, log hashes and sampled memory. Different
storage and execution conditions prevent treating these wall times as an isolated
DuckDB-versus-Spark performance comparison. No higher scale was generated.
