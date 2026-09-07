# Candidate comparisons and experiments

NDC compares named candidates through validated result cells. A candidate may be an
engine version, build or configuration. Labels do not define an engine plugin API:
the candidate command must implement the existing result and workload contracts.
See [engine scripts](engines.md#engine-scripts) for runnable implementations.

## Compare completed results

From the repository root:

```sh
./ndc/run.sh compare base=base-result.json candidate=candidate-result.json --out comparison.md
```

Supply explicit result JSON files, not campaign directories. Repeat a label to
include independent passes. The first label is the baseline. Comparison requires
matching dataset, physical format, workload, frozen schedule, measurement settings
and harness source identities, plus complete validated answers. Pass counts must
match; a candidate cannot change artifacts or declared build metadata between
passes. Different candidate artifact hashes are expected. Failed campaigns and
missing or invalid samples are rejected. Existing output files are never replaced.

Reports include per-query medians and ranges, descriptive totals, artifact hashes,
declared revisions/build profiles, and recorded resource limits. They omit machine
names and input paths. Timings remain exploratory: multiple samples alone do not
establish statistical significance, controlled caches or an isolated machine.

## Explain and enforce plan changes

`--operator OPERATOR` reports queries containing that operator in every sample,
including gains and losses against the baseline. Names come from the engine's
saved `operator_metrics` inventory. `--require-operator LABEL QUERY OPERATOR` fails
comparison if any sample lacks the required operator. Repeat either option as
needed. Missing or unsafe plan paths fail these checks; unavailable evidence is
not interpreted as zero coverage. Operator presence does not prove every operator
in the query is native, nor that a particular predicate was pushed down.

## Run candidates sequentially

`./ndc/run.sh experiment candidates.json --out experiment --mode correctness`
executes one pass in configured candidate order with no warmups. Use
`--mode performance` for warmed, repeated fresh processes. Its default is twice as
many passes as candidates, with one validated warmup per query in each process.
An explicit `--passes` must be at least two and a multiple of candidate count;
`--warmups` must be positive in performance mode.

The initial performance order is seeded, then rotated so every candidate occupies
every position equally. Query order uses the same seed across candidates/passes.
`--seed` controls both schedules. Runs are sequential; the workflow does not create
machine isolation, drop caches or establish a TPC execution protocol.

The JSON specification has a `candidates` list. Each entry provides a unique
`label`, a nonempty `command` argument array, and optional string-valued `env`,
`revision` and `build_profile` fields. Commands run from the specification's parent
directory, without a shell. Treat commands and configuration as trusted executable
inputs. Do not put credentials in commands or build metadata.

Each command must honor `NDC_CAMPAIGN_DIR`, `RUNS=1`, `STREAMS=1`, `WARMUPS`,
`QUERY_SEED`, and `NDC_RUN_INTENT`, and produce exactly one valid result cell.
Candidate metadata is supplied through `NDC_CANDIDATE`, `NDC_CANDIDATE_REVISION`
and `NDC_BUILD_PROFILE`. The bundled implementation records these declared values
alongside independently computed artifact checksums. Declarations do not verify
that an artifact was built from that revision or profile.

`experiment.json` records order, stage status and relative evidence filenames.
Each command has a full log; lines beginning with `NDC ` are forwarded live.
Command/validation failures stop execution and preserve diagnostics. Successful
experiments write `comparison.md`. Operator options also work with `experiment`.

For allocated resources, candidate configuration and a runnable specification,
see the [bundled runner guide](../ndc/README.md#candidate-experiments).
