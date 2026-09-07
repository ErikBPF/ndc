# Implementation review — 2026-09-07

Scope: the NDC expansion before baseline commit `6dfdd32`, including workload contracts, runner,
validation, reporting, evidence packaging, CI and documentation. Reviewed against
the requested nested-data scan/read/write/compute benchmark and its TPC-H-derived,
TPC-DS-inspired positioning. No official TPC compliance or performance claim.

## Verified findings and fixes

| Priority | Finding | Fix and regression evidence |
|---|---|---|
| High | Removing a workload from every result cell still produced a report: expected coverage was inferred from surviving samples. | Cells now embed the measured manifest, including SQL text. Reporter verifies its identity and requires every declared workload/repetition/stream. Regression tests cover omitted workloads and altered identities. |
| Medium | Sizing accepted a reference CSV missing required table columns because comparisons used the intersection. | Require all eight reference and live columns and exactly one live count row. Regression tests cover incomplete references. |
| Medium | Bundling an old campaign after dataset regeneration silently attached the new inventory. | Check dataset inventory content identity and relevant format markers against measured cells before creating the archive. Regression tests cover stale inventories. |

Bundle source validation now hashes the bytes selected for the archive. This also
avoids reading arbitrary source paths supplied in a result record and detects
measured files missing from the archive. Shared identity hashing is reused; no
new dependencies or framework were introduced.

## Compatibility and deferred concerns

Earlier schema-v2 cells lacking embedded manifests require their original reporter;
they are rejected by the current reporter. Documentation states this compatibility
boundary.

Considered but did not treat as defects: Parquet UPDATE/DELETE are explicitly
unsupported; structural Comet operator fractions are not timing fractions; write
plans explicitly describe verification reads. Those limits are already disclosed.

Limits at the reviewed baseline: full-output oracles had to fit driver memory; process sampling can
miss short-lived work; selected runtime settings are recorded rather than every
Spark setting. The Spark installation marker trusts a previously installed local
cache; it does not rehash the extracted distribution on every use. Qualification
runs do not establish performance significance. GitHub-hosted correctness and security subsequently passed for baseline commit
`6dfdd32`; see [published check records](validation.md#published-check-records).

Later changes added distributed validation; see the [current measurement rules](methodology.md#distributed-validation).

## Validation

The [baseline correctness run](https://github.com/ErikBPF/ndc/actions/runs/34086563045)
passed 25 standard-library tests, ShellCheck, Bash syntax and hosted integration.
Regression coverage is available in [harness tests](../tests/test_harness.py) and
[provenance tests](../tests/test_provenance.py).
