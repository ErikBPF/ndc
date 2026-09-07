# Benchmark validation — 2026-09-07

**Qualification passed. These are correctness checks, not new performance claims.**
NDC's TPC-H-derived queries and TPC-DS-inspired/synthetic workloads were exercised
with Spark 4.1.3, Comet 1.0.0, Iceberg 1.11.0 and Delta 4.3.1.

## Results

| Check | Observed result |
|---|---|
| Final harness tests | 21 standard-library tests passed; ShellCheck and Bash syntax passed |
| Fresh artifact installation | Spark SHA-512 and four JAR SHA-256 pins verified; cached setup rerun passed |
| Tiny TPC-H generation | All eight table counts passed at declared sf0.0083; 48,214 line items, 12,000 orders |
| Structural invariants | Exact retained-leaf bags and parent cardinality passed |
| Independent qualification | All 24 answer pins matched eight DuckDB flat-reference queries |
| Repeated complete matrix | 46 cases × 6 cells × 2 repetitions: **544 passed, 8 unsupported** |
| Final-source complete matrix | 46 cases × 6 cells × 1 repetition: **272 passed, 4 unsupported** |
| Wrong-answer injection | Actual Spark query returned a deliberately wrong E18 answer; invalid record and nonzero exit observed |
| Concurrent DS-inspired reads | 2 queries × 2 streams × 2 repetitions × 2 engines: **16 passed**, with validated warm-ups |
| Empty/null shape boundary | 22 synthetic/read/write/maintenance cases × 2 engines: **40 passed, 4 unsupported** |

All unsupported samples are Parquet UPDATE/DELETE, which have no transactional
implementation. They are explicit capability results, not failed tests hidden as
success. Every supported sample in the successful campaigns passed complete
answer validation. Reports passed cross-engine/cross-format answer identities,
coverage, source, input and configuration compatibility checks.

The first development matrix exposed Iceberg Hadoop-catalog custom-location
rejection. Writes were moved to a per-cell scratch catalog; both the repeated and
final matrices then passed. The failed development campaign remains preserved.

## Environment and limits

- Allocated test scope: CPU quota 400% (up to four CPUs of aggregate compute),
  with a 16 GiB memory cap. This is the enforced allocation, not total machine capacity.
- Spark: `local[4]`, 8 GiB driver heap; Comet has a configured 2 GiB off-heap pool.
- All Spark acceptance phases: transient user scope with CPU quota 400% and
  `MemoryMax=16G`. The scope does not restrict affinity to four particular CPUs.
- Main matrices: matched formats, seed 7, uncontrolled cache, no host-wide cache
  drops. Repeated matrix used two repetitions; final-source matrix used one.
- Main synthetic input: 128 parents, maximum fan-out 64, padding width 8.
- Boundary input: one parent, fan-out 1, width 0; nullable padding placeholder and
  empty/null nested collections. Both engines exercised the same data.
- Throughput: Parquet, two shared-application query streams, two repetitions,
  one validated warm-up, `CACHE=warm`. Per-query resource attribution is unavailable
  for concurrent streams by design.

Fresh Spark/JAR artifacts were installed in `verified-spark/` under the validation
root. Tests used separate workspaces.

## Retained evidence

Paths below are relative to the retained validation root; campaign IDs identify
the evidence independently of machine names.

| Phase | Workspace-relative campaign |
|---|---|
| Repeated development acceptance | `workspaces/tpch-tiny/results/20260907T041624-ff11686e` |
| Final-source acceptance | `workspaces/tpch-tiny/results/20260907T043512-76c54da5` |
| Wrong-answer injection | `workspaces/tpch-tiny/results/negative_sbsgdvhl` |
| Concurrent streams | `workspaces/tpch-tiny/results/20260907T044118-e1922c3a` |
| Empty/null boundary | `workspaces/tpch-empty/results/20260907T044206-bc78f6dc` |

Final measured source identity:
`cbe07a3be0ea05d3ede059fd96872b807b71794cc405994b8edf8ed353aff36c`.
The earlier repeated campaign used source identity
`a487e1d695b820eaa251134afe34446c2b7eb399ac0a5fe7a30583942a58c917`.
The final pass additionally exercised the isolated `nix/` environment, protected
existing campaign manifests, and fresh verified engine artifacts.

The final source snapshot is retained in `final/`; the earlier snapshot is in
`code/`. Each cell records its complete source-file hash inventory, JAR hashes,
configuration, dataset identity, command and query plans. Dataset identities bind
physical input files; they are not claims of byte-identical regeneration by Spark.

`run.sh bundle <campaign>` packages source and diagnostic evidence with per-file
checksums and an archive `.sha256` sidecar. Final, throughput and boundary bundles
are staged beside their campaign directories, with local copies under the `ndc`
checkout's ignored `results/validation/`. Source scripts preserve executable modes.
The archive includes data inventories, not the potentially large Parquet payloads.

GitHub-hosted qualification subsequently passed for baseline commit `6dfdd32`:
[benchmark correctness](https://github.com/ErikBPF/ndc/actions/runs/34086563045)
and [security](https://github.com/ErikBPF/ndc/actions/runs/34086563026).
The hosted PR job passed 25 harness tests, pinned runtime installation, tiny data
qualification, the vanilla/Parquet workload suite, and wrong-answer rejection.
Its diagnostic artifact has 14-day retention. The six-cell validation remains the
test evidence above; scheduled/manual CI selects all engines and formats.
No release or public performance result was published. A durable public artifact
URL must be added when publishing a future benchmark claim.

## Review revision

The implementation review identified and fixed manifest coverage, sizing-reference
coverage and stale bundle-inventory gaps; see [the review](implementation-review.md).
The initial acceptance above remains evidence for the preserved `final/` snapshot.
The revised source is retained separately in `rv/`.

- Harness: **25 tests passed** locally and on the test runner, including four
  regressions first observed failing. ShellCheck and Bash syntax checks passed.
- Revised complete matrix: **272 passed, 4 explicitly unsupported**, all 46 cases
  across both engines and three formats, one repetition, no warm-up. Same prepared
  tiny dataset, resource limits and pinned artifacts as initial acceptance.
- Campaign: `workspaces/tpch-tiny/results/20260907T045852-9285daa3`.
- Source identity: `15ccab3a7f70f7e89322e9bcd18c00117367a6f89344d6b167a68ac9eda481c8`.
- Revised reporter accepted the complete embedded manifests and all comparisons.
- Actual Spark wrong-answer injection again produced invalid status and nonzero exit.
- Logs: `rv-harness.log`, `rv-matrix.log`, `rv-negative.log`.

Earlier cells lack embedded manifests and must use their preserved original
reporter. They were not rewritten to satisfy the revised validity gate.

The revised campaign bundle and SHA-256 sidecar are retained beside the campaign
and copied to ignored `results/validation/` locally. Archive and every member
checksum passed; the measured source inventory matches the current working tree.
