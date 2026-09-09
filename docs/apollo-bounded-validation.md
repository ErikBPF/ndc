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

Pre-scale review validation: 70 repository tests plus shell checks passed locally and
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

## Final review

Two reproduced issues were fixed after the scale runs. Both importers now inspect
combined Parquet schemas before validating columns: an extra column present only
in a later part previously disappeared silently. Legacy nesting now forwards
DuckDB stderr so failed SQL exposes its actual diagnostic.

Regression checks failed before the fixes and passed afterward. The final code
passed 72 repository tests plus shell checks locally and on Apollo, and the Spark
fixture suite at local[2] / 2 GiB, including mixed-schema rejection. The scale
results above predate these validation/diagnostic fixes; SF0.5 and SF10 were not
regenerated during this review. Their inputs had uniform schemas, and the nesting
algorithm and resource settings are unchanged.

## Kubernetes SF1 on ext4 (2026-09-09 UTC)

Spark 4.1.3 preparation passed SF1 with a driver pod and two executor pods on
Apollo's `apollo-dev` cluster, one executor on each worker (`w-1`, `w-2`).
Execution plus standalone checksum verification took **170 seconds**, excluding
flat generation and image pulls. All 1,500,000 orders and 6,001,215 line items
passed exact round-trip validation. This qualifies preparation, not benchmark
query execution or SF1000.

Each JVM used a 2 GiB heap inside a 3 GiB pod limit. The driver requested 1 CPU;
each executor requested 500m CPU, with a 2 CPU limit and two Spark task slots.
Existing workloads reserved most worker CPU; a first attempt requesting 2 CPU
per executor was stopped when its second executor could not schedule. Partial
output was retained separately. This is a shared-cluster functional test, not an
isolated throughput measurement.

Metrics Server samples, polled every 10 seconds, reached approximately 1.32 GiB
for the driver, 2.84 and 2.93 GiB for the executors, and 7.05 GiB summed across
pods. These are sampled container working sets, not process RSS or guaranteed
peaks. All final pods completed without OOM or restarts; executor memory was
close to its limit, so these settings do not establish SF1000 sizing.

The official image was pinned by digest (see manifest), with Python 3.10.12 and
Java 17.0.19. Its Python version exposed a checksum compatibility defect:
`hashlib.file_digest` requires Python 3.11. Commit `838160f` replaces it with
streaming SHA-256 in 1 MiB blocks. All 73 repository tests and shell checks passed
on Apollo; the checksum regression also passed inside the Python 3.10 image.
The tested source matches that commit.

The cluster's default local-path PVC is backed by Btrfs. For this test, source,
output and Spark spill directories instead used a temporary NFSv4.1 export of
`/mnt/microvms/ndc-validation/k8s-sf1-20260909` on Apollo's ext4 disk. Export access
was restricted to the two worker IPs with root squashing. The NixOS workers
needed the explicit `addr=10.251.0.1` mount option. The PVC's advertised 20 GiB
capacity is not an enforced NFS quota. This temporary export is separate from
the previously unavailable external NFS server.

[Machine-readable results](evidence/apollo-k8s-sf1-20260909.json) include resource
settings, counts, timing and artifact hashes. The
[exact Kubernetes manifest](evidence/apollo-k8s-sf1-manifest.json) records RBAC,
shared volume, driver command and executor placement. Reusing it requires a new
run directory/output path, prepared flat inputs, reachable NFS export and the
`ready` marker; it is evidence for this run, not a portable deployment profile.
The ext4 run directory retains datasets, logs and resource samples.
Temporary namespace, PVC/PV and NFS export were removed after evidence capture.
No test pods or NFS server threads remain.
