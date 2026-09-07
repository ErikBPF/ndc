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

## Choose regression coverage by change

| Change | Required coverage |
|---|---|
| Nested null-predicate scan eligibility | List/map NULL and NOT NULL, empty collections, null elements, struct fallback and native/fallback plan assertions |
| Nested projection or schema matching | Selected fields, missing fields, case handling, schema evolution and calendar policies |
| Deletion-vector row-group skipping | Deleted and retained rows across row-group boundaries, with exact expected row sets |
| Deletion-vector representation or selection | Sparse, dense and contiguous deletions, empty/full selections and memory bounds |

Distinguish scan eligibility from residual predicate pushdown. A native scan does
not prove that list/map predicates execute inside the storage reader; a filter
above the scan can enforce them. Inspect the executed plan and predicate handling
in the tested source before claiming pushdown. NDC’s ordinary Delta writes do not
enable deletion vectors or load the optional contrib module; use dedicated
upstream fixtures to qualify deletion-vector changes.

## SF1 campaign procedure

Pin the candidate revision and its comparison base, matching Spark/Scala/Iceberg versions and
native build profile. Build each candidate in an isolated checkout; capture the
source revision and JAR checksum. For a JVM-only change, reusing a native library is
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
accessible check runs or immutable bundles; identify the exact source and artifacts used.

## Security and developer experience

Review and pin candidate source before building it. Do not automatically execute
arbitrary fork PR code on a credential-bearing test runner. Benchmark commands run
SQL, candidate JARs and workspace `bench.conf` shell configuration; those inputs
must be trusted. `--help` and `check` do not source workspace configuration.

Use `./ndc/run.sh --help` for entry points and `plan` to inspect a schedule before
execution. Both runner and reporter share the supported-capability rules: an
unsupported marker cannot replace a failed read. Relative workspace paths resolve
once and remain stable across child commands.
