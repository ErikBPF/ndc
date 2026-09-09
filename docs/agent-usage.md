# Use NDC with a coding agent

Copy and paste this prompt into your agent. Fill in the task details you know;
the agent should resolve the rest from your instructions and the repository.

```text
Use Nested Data Compute (NDC) to validate the following benchmark task.

Task: [behavior or change to evaluate]
Engine/candidates: [implementation, baseline and candidate revisions/artifacts]
Workloads/formats: [requested coverage, or select relevant suites]
Scale/resources: [data scale, CPU, memory, disk and execution environment]
Intent: [correctness or performance; default to correctness]

Read the repository instructions and README.md, then docs/engines.md,
docs/workloads.md, docs/methodology.md, docs/experiments.md and
docs/validation.md. For shared version 2 inputs, also read docs/data-contract.md
and datagen/README.md; verify the dataset before loading it. The runner and adapters use the same canonical data contract. Follow the selected
engine's setup and command guide.
Use the checked-out source and CLI help to verify commands. NDC's contracts
are engine-independent; do not assume its bundled implementation supports
an arbitrary engine, dialect or format. Report missing support explicitly.

Use existing instructions and artifacts to resolve task details. Ask only
for information that blocks correct execution. State assumptions and the
selected workload, scale and resource allocation before expensive work.
Start with tiny correctness qualification when intent or scale is unspecified.
Do not silently turn a correctness task into a performance campaign.

Pin harness and candidate sources, runtime dependencies and build profiles;
record artifact checksums. Use trusted commands, artifacts and workspace
configuration. Preserve existing work and use fresh output directories.
Enforce the allocated CPU/memory limits, account for engine memory settings,
and check disk capacity. Do not drop host-wide caches without authorization.

Run repository checks and the implementation's tiny qualification gates
before larger measurements. For engine changes, run relevant upstream
regressions too. Prepare and validate matched physical inputs. Record
synthetic shape parameters separately from the TPC-H scale factor.
Validate every complete answer, including nulls, duplicates and ordering;
for writes, validate committed state. Preserve workload semantics and
reference answers. Never weaken validation or hide failed/unsupported cases.

For comparisons, keep data, formats, workloads, harness source and measurement
settings compatible. Use the documented compare/experiment commands where
the implementation supports their result contract. Correctness runs do not
establish speedups. Performance work requires warmups, repeated fresh processes
and balanced candidate order; disclose cache state and competing workloads.
Keep latency, concurrency and maintenance experiments distinct.

When execution-path claims matter, inspect saved plans and assert the intended
operators or fallback. Correct answers alone do not prove native execution;
native scanning alone does not prove predicate pushdown. Stop on invalid
evidence, preserve diagnostics and explain the failure before comparing timings.

Report coverage, exclusions, correctness, per-query medians/ranges and sample
counts, resource allocation, artifact identities and limitations. Distinguish
observations from explanations and statistical claims. NDC is TPC-H-derived
and TPC-DS-inspired, not compliant with either benchmark; do not report a TPC
score or compare with published TPC results.

Keep repository documentation reusable: no PR inventories, host names or
session narratives. For shared results, cite accessible checks for the exact
revision or complete immutable evidence bundles, not private paths or IDs.
Prepare evidence for review; publish, push or merge only within the user's
authorization. Finish with what passed, what failed and what remains unverified.
```

For command examples, use the [engine guides](engines.md#engine-scripts) and
[candidate experiment contract](experiments.md). The prompt guides execution;
it does not replace those contracts or qualify results by itself.
