# Benchmark contract and engine implementations

NDC is intended for multiple query engines. Spark is the initial implementation
and the current development use case. Comet is an execution mode within Spark;
neither Spark APIs nor its session model define the benchmark’s scope.

## Engine scripts

| Implementation | Setup and commands | Entry point |
|---|---|---|
| Spark, with optional Comet | [Runner guide](../ndc/README.md) | [`ndc/run.sh`](../ndc/run.sh) |

For named result comparisons and balanced command execution, see
[candidate experiments](experiments.md). These reuse the existing result contract;
they do not add an engine execution plugin API.

For the bundled runner, see [Comet PR and SF1 validation](comet-validation.md).

Each implementation documents its own dependencies, supported formats, execution
settings and qualification commands. The table lists scripts available in this
repository; it does not restrict which engines can implement NDC.

## What another engine must preserve

- **Workload identity and semantics:** retain workload IDs, logical input fields,
  operation, expected result and declared ordering from the
  [workload contracts](workloads.md) and [manifests](../ndc/queries/manifest-all.json).
  Translate SQL and nested types to the engine’s dialect without changing nulls,
  duplicate multiplicity, array order, decimal precision or top-N semantics.
- **Complete validation:** validate every measured result, with explicit failures
  and unsupported capabilities. Use the [tiny answer pins](../ndc/answers/README.md)
  where applicable and equivalent reference/oracle checks elsewhere. Validation
  may collect, stream or execute within the engine; disclose its timing boundary.
- **Reproducible execution:** record actual query text, input identity, query order,
  seeds, warm-ups, repetitions and concurrency model before measurement. Reuse
  [schedule generation](../ndc/schedule.py) where its manifest contract fits.
- **Write state:** preserve the specified initial state, mutation and expected final
  state. Time the declared completion/commit boundary and validate committed output.
  Report unsupported formats or transactions instead of silently substituting them.
- **Comparable disclosure:** publish engine/version, format and physical layout,
  configuration, allocated resources, timing boundary and validation mode. Publish
  raw samples and complete evidence with any performance claim. Engine-specific
  metrics are optional diagnostics; unavailable counters are not zero.

A supported subset is useful if its membership and exclusions are explicit.
Cross-engine comparisons require equivalent semantics and measurement boundaries;
matching workload names alone is insufficient. Materialization in driver memory
and executor disk materialization are different timing modes.

## Current implementation boundaries

The repository does not yet provide a generic runner plugin API. Adding a value to
`ENGINES` does not add an engine. The following code is Spark-specific today:

| Component | Current constraint |
|---|---|
| [CLI](../ndc/run.sh) and [qualification](../ndc/qualification.py) | Launch Spark; accept `vanilla` and `comet` |
| [Query execution](../ndc/spark_poc.py) | Spark SQL, sessions, plans and output handling |
| [Synthetic data](../ndc/shapes.py) and [format preparation](../ndc/write_fmt.py) | Spark schemas and writers; generation is separate from timed execution |
| [Mutations](../ndc/mutations.py) | Spark storage/catalog APIs |
| [Validation](../ndc/validation.py) | Python canonicalization plus Spark RDD support; no portable validator API |
| [Reporter](../ndc/report.py) | Reads `spark_*.json`, checks Spark versions and renders Spark/Comet ratios |
| [CI](../.github/workflows/ci.yml) | Qualifies the implemented Spark configurations |

These are implementation gaps for a new engine, not restrictions on which engines
may implement NDC. The existing reporter cannot establish cross-engine comparability:
it requires the same SQL-bearing manifest identity and Spark version. A dialect
translation changes that identity even when its semantics are equivalent.

## Adding an implementation

Start with a bounded read suite and the tiny qualification data. Implement the
engine’s query translations, data loading and complete-answer checks. Keep query
execution and engine metrics in that implementation; reuse existing logical
contracts and fixtures where possible. Input preparation can be external, but its
provenance and effect on physical layout must be disclosed.

Add a runnable qualification test, including a wrong-answer case, before publishing
measurements. Extend orchestration and reporting for the new engine explicitly:
separate semantic workload identity from dialect-specific query identity, record
engine/version without assuming Spark, and select comparison baselines explicitly.
Do not label another engine’s output as `vanilla` to pass the current report gate.
Add writes, maintenance and concurrency as supported capabilities with their own
validated semantics and disclosed execution model.

See [measurement rules](methodology.md) and [validation evidence policy](validation.md#citing-evidence).
