# Qualification answers

`sf0.0083/*.out` contains pipe-separated qualification results. `\N` is the explicit
null marker. Column types come from the declared reference query schema; decimal
precision and duplicate rows are preserved. These are NDC answers, not official
TPC-H or TPC-DS reference answers.

The original answer files lack recorded engine/tool provenance. They are not
independently regenerated historical evidence. The suite verifies every pin by executing
eight distinct flat-reference queries with the pinned DuckDB against freshly
built sf0.0083 data (`./ndc/run.sh qualify-references`). The Spark runners then
verify every one of the 24 query variants against these pins.

Pinned semantics:

- Q6 flat and higher-order variants include `cnt=940` alongside exact
  `revenue=905922.8234`, matching the already pinned depth answers.
- E18 depends on both selected items and returns two columns.
  The pinned answer is `10996|1027906826.10|` for qualifying-order
  count and total top-two revenue.
- Other pins retain their original contents. The reference-qualification command
  checks them against DuckDB; see [validation and CI](../../docs/validation.md).

Never regenerate answers automatically during CI or a measured run. A semantic
change requires reviewing its reference SQL and independently recomputing the
expected result. `qualify.py` only validates; it cannot overwrite pins.
