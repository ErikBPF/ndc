# Qualification answers

`sf0.0083/*.out` contains pipe-separated qualification results. `\N` is the explicit
null marker. Column types come from the declared reference query schema; decimal
precision and duplicate rows are preserved. These are NDC answers, not official
TPC-H or TPC-DS reference answers.

The original answer files were present before this revision. Their original
engine/tool provenance was not recorded, so they are not claimed as independently
regenerated historical evidence. The revised suite verifies every pin by executing
eight distinct flat-reference queries with the pinned DuckDB against freshly
built sf0.0083 data (`run.sh qualify`). The Spark runners then verify every one of
the 24 query variants against these pins.

Changes in this revision:

- Q6 flat and higher-order variants now include `cnt=940` alongside exact
  `revenue=905922.8234`, matching the already pinned depth answers.
- E18 now depends on both selected items and returns two columns. Independent
  DuckDB execution on Apollo produced `10996|1027906826.10|` for qualifying-order
  count and total top-two revenue.
- Other pins retained their original contents and were independently checked on
  Apollo; see [validation evidence](../../docs/validation-apollo.md).

Never regenerate answers automatically during CI or a measured run. A semantic
change requires reviewing its reference SQL and independently recomputing the
expected result. `qualify.py` only validates; it cannot overwrite pins.
