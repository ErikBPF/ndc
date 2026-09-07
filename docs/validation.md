# Validation and CI

The bundled Spark runner’s correctness checks are defined in the repository and executed by
[benchmark correctness CI](../.github/workflows/ci.yml).
[Security CI](../.github/workflows/security.yml) runs separately.
These checks do not establish performance claims or TPC compliance.

## Published check records

| Revision | Correctness | Security |
|---|---|---|
| Baseline `6dfdd32` | [Passed](https://github.com/ErikBPF/ndc/actions/runs/34086563045) | [Passed](https://github.com/ErikBPF/ndc/actions/runs/34086563026) |
| Distributed validation `2f6b06f` | [Passed](https://github.com/ErikBPF/ndc/actions/runs/34088141785) | [Passed](https://github.com/ErikBPF/ndc/actions/runs/34088141756) |

For newer revisions, inspect the [correctness workflow runs](https://github.com/ErikBPF/ndc/actions/workflows/ci.yml)
and [security workflow runs](https://github.com/ErikBPF/ndc/actions/workflows/security.yml).
A pending or failed run is not qualification evidence. Check the revision and
individual jobs before citing a result.

## Reproducible checks

- `./ndc/run.sh check`: standard-library regression tests, ShellCheck and Bash syntax.
- `./ndc/run.sh qualify-engine vanilla parquet`: on a prepared tiny workspace,
  sizing, structural invariants, reference pins, parity and all candidate workloads.
  Explicitly unsupported operations remain visible in the report.
- [Wrong-answer integration test](../tests/integration.py): verifies invalid output
  fails both validity and process exit; CI executes both validation modes.
- [Spark validation tests](../tests/spark_validation.py): nested bags, ordering,
  duplicates, stable answer identities, write round trips and driver result limits.

Follow the [quickstart](../README.md#spark-quickstart-linux-x86-64) for setup and data
preparation. See [commands and configuration](../ndc/README.md) for engine/format
selection and [measurement rules](methodology.md) for timing and resource limits.
Scheduled/manual CI selects vanilla Spark and Comet across all three formats; routine PR CI
qualifies vanilla/Parquet.

## Citing evidence

Link to an accessible check run for the exact revision. For benchmark claims,
publish a complete immutable evidence bundle with source, dataset inventory,
configuration, raw results and checksums, then cite its published URL. Machine-local
paths, private campaign IDs and session IDs are not reader-accessible evidence.
CI diagnostic artifacts expire after 14 days and are not a durable benchmark archive.
