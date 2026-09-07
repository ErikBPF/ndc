# Validating nesting changes in Comet

This guide covers the bundled engine scripts. It complements the engine-independent
[NDC contracts](engines.md); it does not make Comet’s execution model a benchmark
requirement.

## Upstream test layers

Comet’s [Linux PR workflow](https://github.com/apache/datafusion-comet/blob/main/.github/workflows/pr_build_linux.yml)
builds native code before JVM tests and explicitly registers Scala suites across
Spark/JDK profiles. Its SF1 TPC-H result suite and SF1 TPC-DS result suite validate
relational queries; TPC-DS is exercised with multiple join strategies. Those checks
are distinct from NDC’s nested workloads.

The [development guide](https://github.com/apache/datafusion-comet/blob/main/docs/source/contributor-guide/development.md)
explains build order and suite selection. Run Maven from the reactor root; avoid
`-pl` and empty `-DwildcardSuites`. Check that the expected suite and a nonzero test
count ran. `-DskipTests` produces an artifact, not test evidence.

[SQL file tests](https://github.com/apache/datafusion-comet/blob/main/docs/source/contributor-guide/sql-file-tests.md)
compare Spark answers and can assert native execution or explicit fallback.
[Iceberg integration tests](https://github.com/apache/datafusion-comet/blob/main/docs/source/contributor-guide/iceberg-spark-tests.md)
exercise upstream Iceberg against Comet. Scan-rule changes need both answer parity
and the intended native/fallback plan; an unchanged answer can hide a full fallback.

## Relevant contributions

| PR | Scope | Relevant validation |
|---|---|---|
| [apache/datafusion-comet#5732](https://github.com/apache/datafusion-comet/pull/5732) | Struct-only Iceberg null-predicate fallback; authored contribution | List/map NULL and NOT NULL, empty collections, retained struct fallback, SF1 nested Iceberg scans |
| [apache/datafusion-comet#5365](https://github.com/apache/datafusion-comet/pull/5365) | Native Delta scan; review and workload-validation contribution | Contrib suite, nested projection, calendar policies and deletion-vector-specific fixtures |
| [dwsmith1983/datafusion-comet#1](https://github.com/dwsmith1983/datafusion-comet/pull/1) and [#2](https://github.com/dwsmith1983/datafusion-comet/pull/2) | Deleted row-group skipping and ASCII field matching; authored contributions | DV row-group boundaries and schema-name matching, respectively |
| [dwsmith1983/datafusion-comet#3](https://github.com/dwsmith1983/datafusion-comet/pull/3) and [#4](https://github.com/dwsmith1983/datafusion-comet/pull/4) | Compressed DV representations/selections; authored contributions | Sparse/dense/range DV fixtures with expected row sets and memory bounds |

For #5732, the [review discussion](https://github.com/apache/datafusion-comet/pull/5732)
distinguishes scan eligibility from residual pushdown. Native scanning does not
prove list/map predicates execute inside iceberg-rust; the filter above the scan
can enforce them. Inspect the current source and review thread before making a
pushdown claim. NDC’s normal Delta writes do not enable deletion vectors or load
the optional contrib module, so their success does not qualify the DV changes.

## SF1 campaign procedure

Pin the PR head and its comparison base, matching Spark/Scala/Iceberg versions and
native build profile. Build each candidate in an isolated checkout; capture the
source revision and JAR checksum. For a JVM-only PR, reusing a native library is
valid only when the native source and build settings are identical between candidates.

First run the affected upstream suites and NDC tiny candidate qualification using
that candidate JAR. Then prepare a fresh SF1 workspace from the NDC repository root:

```sh
./ndc/run.sh bootstrap 1 nesting
export NDC_WORKSPACE="$PWD/workspaces/tpch-nesting"
FORMATS='parquet iceberg' ./ndc/run.sh build-scale

# COMET_JAR must name the reviewed candidate built for the runner's Spark/Scala versions.
FORMATS=iceberg SUITE=read RUNS=1 WARMUPS=0 ./ndc/run.sh latency
```

Repeat the campaign with the base JAR in the same prepared workspace; each command
creates a fresh result directory. The default engine pair checks vanilla Spark and
Comet. Retain complete answers and executed plans; compare artifact hashes as well
as source revisions. For outputs beyond driver memory, select
`VALIDATION=distributed` consistently across the compared runs.

Use explicit CPU and memory limits. Record execution threads, driver heap, Comet
off-heap memory, cache mode and build profile. One repetition without warm-ups is
a correctness check, not a performance comparison. Publish evidence only through
accessible check runs or immutable bundles; never cite private campaign/session IDs.

## Security and developer experience

Review and pin candidate source before building it. Do not automatically execute
arbitrary fork PR code on a credential-bearing test runner. Benchmark commands run
SQL, candidate JARs and workspace `bench.conf` shell configuration; those inputs
must be trusted. `--help` and `check` do not source workspace configuration.

Use `./ndc/run.sh --help` for entry points and `plan` to inspect a schedule before
execution. Both runner and reporter share the supported-capability rules: an
unsupported marker cannot replace a failed read. Relative workspace paths resolve
once and remain stable across child commands.
