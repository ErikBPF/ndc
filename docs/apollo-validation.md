# Apollo validation: SF0.5 and SF10

On 2026-09-08, source commit `f316bf295ba29f51030efed61d20f08f358538d5`
was tested on Apollo. Only SF0.5 and SF10 were generated. These are preparation
and correctness checks, not a performance comparison or SF1000 qualification.

## Results

| Scale | DuckDB limit | Path | Result |
|---|---|---|---|
| SF0.5 | 16 GiB | `build-scale`, Parquet | Passed counts, nested invariants, parity, depths and shapes |
| SF0.5 | 16 GiB preparation | Spark vanilla, 24 TPC-H-derived queries | All 24 samples valid |
| SF0.5 | 16 GiB | Shared importer from flat Parquet, then `--verify` | Passed exact validation and artifact verification |
| SF10 | 16 GiB | `build-scale`, Parquet | Counts/export passed; nesting failed at 15.9/16.0 GiB |
| SF10 | 32 GiB | Fresh-workspace `build-scale` retry | Counts/export passed; nesting failed at 32.0/32.0 GiB |
| SF10 | 16 GiB | Independent shared import from validated flat exports | Failed at 15.9/16.0 GiB; manifest remained failed |

SF10 produced 59,986,052 line items and 15,000,000 orders before nesting failed.
No SF10 Spark query campaign ran because the required nested inputs were incomplete.
The shared import was tested independently using the eight valid flat exports.
The observed budgets are insufficient on this host and revision; they do not
establish the minimum memory needed for SF10.

## Environment and reproduction

Apollo exposed 28 CPUs and 220 GiB RAM. Data, database files, runtime downloads and
logs lived on the NFS fast volume, avoiding its nearly full root disk and tmpfs
`/tmp`. Runtime: DuckDB 1.5.5 (`d8cdaa33fd`), Spark 4.1.3, Java 17.0.20.1,
Python 3.12.14, from the pinned Nix environment and verified runtime artifacts.

Preparation used four DuckDB threads. Spark used `local[4]`, an 8g driver heap,
vanilla/Parquet, `SUITE=tpch`, `RUNS=1`, `WARMUPS=0`, and
`VALIDATION=distributed`. No Comet, Iceberg, Delta or Kubernetes campaign ran.

For each scale, bootstrap a fresh workspace with `./ndc/run.sh bootstrap <sf>
<name>`, select it with `NDC_WORKSPACE`, and run `FORMATS=parquet
NDC_PREP_MEMORY=16GiB ./ndc/run.sh build-scale`. Repeat SF10 in a different
workspace with `32GiB` to reproduce the second attempt. Run the query matrix
only after preparation succeeds. Import the eight flat exports with
`datagen/generate.py --input <workspace>/data --input-format parquet --out
<fresh-directory>` and the selected `NDC_PREP_MEMORY`, then verify successful output.

[Machine-readable evidence](evidence/apollo-20260908.json) records commands, exit
codes, elapsed stages, dataset identities, exact OOM errors and hashes of retained
logs/resource samples. Raw evidence remains on Apollo under
`/mnt/nfs/fast/ndc-validation/pr7-20260908/`, including the `sf10-32g/` retry.
Process-tree RSS is sampled once per second and can double-count shared pages;
it is neither DuckDB-managed memory nor a hard process/cgroup cap.

## Review and revisions

The Apollo checkout passed 66 repository tests, ShellCheck and Bash syntax checks.
After these scale tests, review reproduced a failed-manifest diagnostic defect:
`--verify` rejected an incomplete import with `DATASET_INVALID: 'tables'` because
it accessed table evidence before checking failure status. The revised verifier
reports `DATASET_INVALID: invalid dataset contract`. Regression assertions failed
before the fix and pass afterward; verification continues to reject failed output.

The memory-limit control works, including propagating errors. It does not solve
the ordered-list aggregation's scale limitation. The SF10 retry example has been
replaced with the measured limitation. Distributed preparation remains proposed,
not implemented or qualified.
