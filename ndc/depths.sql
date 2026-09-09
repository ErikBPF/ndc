-- Depth sweep d=1..8 on SF10. All depth tables are Parquet files under data/.
-- depth1: list_transform over the already-built orders_nested.parquet (no aggregation).
-- depth d>1: single-projection wrap of depth d-1, CHAINED from the previous file.
-- Parity: CTE-per-unnest chains (chained FROM-UNNEST does not correlate over TVFs).
-- Invariant: every depth returns the exact flat-Q6 count and revenue.
SET threads TO 4;

COPY (
  SELECT o_orderkey,
         list_transform(lineitems, li -> struct_pack(
             l_shipdate := li.l_shipdate, l_quantity := li.l_quantity,
             l_discount := li.l_discount, l_extendedprice := li.l_extendedprice)) AS c
  FROM read_parquet('data/orders_nested.parquet/*.parquet')
) TO 'data/orders_depth1.parquet' (FORMAT parquet);
COPY (SELECT o_orderkey, struct_pack(inner := c) AS c FROM read_parquet('data/orders_depth1.parquet')) TO 'data/orders_depth2.parquet' (FORMAT parquet);
COPY (SELECT o_orderkey, list_value(struct_pack(inner := c)) AS c FROM read_parquet('data/orders_depth2.parquet')) TO 'data/orders_depth3.parquet' (FORMAT parquet);
COPY (SELECT o_orderkey, struct_pack(inner := c) AS c FROM read_parquet('data/orders_depth3.parquet')) TO 'data/orders_depth4.parquet' (FORMAT parquet);
COPY (SELECT o_orderkey, list_value(struct_pack(inner := c)) AS c FROM read_parquet('data/orders_depth4.parquet')) TO 'data/orders_depth5.parquet' (FORMAT parquet);
COPY (SELECT o_orderkey, struct_pack(inner := c) AS c FROM read_parquet('data/orders_depth5.parquet')) TO 'data/orders_depth6.parquet' (FORMAT parquet);
COPY (SELECT o_orderkey, list_value(struct_pack(inner := c)) AS c FROM read_parquet('data/orders_depth6.parquet')) TO 'data/orders_depth7.parquet' (FORMAT parquet);
COPY (SELECT o_orderkey, struct_pack(inner := c) AS c FROM read_parquet('data/orders_depth7.parquet')) TO 'data/orders_depth8.parquet' (FORMAT parquet);
CREATE OR REPLACE TABLE parity_flat AS SELECT count(*) AS cnt, sum(l_extendedprice * l_discount) AS revenue
FROM lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;
CREATE OR REPLACE TABLE parity_d1 AS
WITH e1 AS (SELECT o_orderkey, unnest(c) AS v1 FROM read_parquet('data/orders_depth1.parquet'))
SELECT count(*) AS cnt, sum(v1.l_extendedprice * v1.l_discount) AS revenue
FROM e1
WHERE v1.l_shipdate >= DATE '1994-01-01' AND v1.l_shipdate < DATE '1995-01-01'
  AND v1.l_discount BETWEEN 0.05 AND 0.07 AND v1.l_quantity < 24;
CREATE OR REPLACE TABLE parity_d2 AS
WITH e1 AS (SELECT o_orderkey, unnest(c.inner) AS v1 FROM read_parquet('data/orders_depth2.parquet'))
SELECT count(*) AS cnt, sum(v1.l_extendedprice * v1.l_discount) AS revenue
FROM e1
WHERE v1.l_shipdate >= DATE '1994-01-01' AND v1.l_shipdate < DATE '1995-01-01'
  AND v1.l_discount BETWEEN 0.05 AND 0.07 AND v1.l_quantity < 24;
CREATE OR REPLACE TABLE parity_d3 AS
WITH e1 AS (SELECT o_orderkey, unnest(c) AS v1 FROM read_parquet('data/orders_depth3.parquet')),
e2 AS (SELECT o_orderkey, unnest(v1.inner.inner) AS v2 FROM e1)
SELECT count(*) AS cnt, sum(v2.l_extendedprice * v2.l_discount) AS revenue
FROM e2
WHERE v2.l_shipdate >= DATE '1994-01-01' AND v2.l_shipdate < DATE '1995-01-01'
  AND v2.l_discount BETWEEN 0.05 AND 0.07 AND v2.l_quantity < 24;
CREATE OR REPLACE TABLE parity_d4 AS
WITH e1 AS (SELECT o_orderkey, unnest(c.inner) AS v1 FROM read_parquet('data/orders_depth4.parquet')),
e2 AS (SELECT o_orderkey, unnest(v1.inner.inner) AS v2 FROM e1)
SELECT count(*) AS cnt, sum(v2.l_extendedprice * v2.l_discount) AS revenue
FROM e2
WHERE v2.l_shipdate >= DATE '1994-01-01' AND v2.l_shipdate < DATE '1995-01-01'
  AND v2.l_discount BETWEEN 0.05 AND 0.07 AND v2.l_quantity < 24;
CREATE OR REPLACE TABLE parity_d5 AS
WITH e1 AS (SELECT o_orderkey, unnest(c) AS v1 FROM read_parquet('data/orders_depth5.parquet')),
e2 AS (SELECT o_orderkey, unnest(v1.inner.inner) AS v2 FROM e1),
e3 AS (SELECT o_orderkey, unnest(v2.inner.inner) AS v3 FROM e2)
SELECT count(*) AS cnt, sum(v3.l_extendedprice * v3.l_discount) AS revenue
FROM e3
WHERE v3.l_shipdate >= DATE '1994-01-01' AND v3.l_shipdate < DATE '1995-01-01'
  AND v3.l_discount BETWEEN 0.05 AND 0.07 AND v3.l_quantity < 24;
CREATE OR REPLACE TABLE parity_d6 AS
WITH e1 AS (SELECT o_orderkey, unnest(c.inner) AS v1 FROM read_parquet('data/orders_depth6.parquet')),
e2 AS (SELECT o_orderkey, unnest(v1.inner.inner) AS v2 FROM e1),
e3 AS (SELECT o_orderkey, unnest(v2.inner.inner) AS v3 FROM e2)
SELECT count(*) AS cnt, sum(v3.l_extendedprice * v3.l_discount) AS revenue
FROM e3
WHERE v3.l_shipdate >= DATE '1994-01-01' AND v3.l_shipdate < DATE '1995-01-01'
  AND v3.l_discount BETWEEN 0.05 AND 0.07 AND v3.l_quantity < 24;
CREATE OR REPLACE TABLE parity_d7 AS
WITH e1 AS (SELECT o_orderkey, unnest(c) AS v1 FROM read_parquet('data/orders_depth7.parquet')),
e2 AS (SELECT o_orderkey, unnest(v1.inner.inner) AS v2 FROM e1),
e3 AS (SELECT o_orderkey, unnest(v2.inner.inner) AS v3 FROM e2),
e4 AS (SELECT o_orderkey, unnest(v3.inner.inner) AS v4 FROM e3)
SELECT count(*) AS cnt, sum(v4.l_extendedprice * v4.l_discount) AS revenue
FROM e4
WHERE v4.l_shipdate >= DATE '1994-01-01' AND v4.l_shipdate < DATE '1995-01-01'
  AND v4.l_discount BETWEEN 0.05 AND 0.07 AND v4.l_quantity < 24;
CREATE OR REPLACE TABLE parity_d8 AS
WITH e1 AS (SELECT o_orderkey, unnest(c.inner) AS v1 FROM read_parquet('data/orders_depth8.parquet')),
e2 AS (SELECT o_orderkey, unnest(v1.inner.inner) AS v2 FROM e1),
e3 AS (SELECT o_orderkey, unnest(v2.inner.inner) AS v3 FROM e2),
e4 AS (SELECT o_orderkey, unnest(v3.inner.inner) AS v4 FROM e3)
SELECT count(*) AS cnt, sum(v4.l_extendedprice * v4.l_discount) AS revenue
FROM e4
WHERE v4.l_shipdate >= DATE '1994-01-01' AND v4.l_shipdate < DATE '1995-01-01'
  AND v4.l_discount BETWEEN 0.05 AND 0.07 AND v4.l_quantity < 24;

.mode json
.output results/depths.json
SELECT 'flat' AS q, cnt, revenue FROM parity_flat UNION ALL SELECT 'd1' AS q, cnt, revenue FROM parity_d1 UNION ALL SELECT 'd2' AS q, cnt, revenue FROM parity_d2 UNION ALL SELECT 'd3' AS q, cnt, revenue FROM parity_d3 UNION ALL SELECT 'd4' AS q, cnt, revenue FROM parity_d4 UNION ALL SELECT 'd5' AS q, cnt, revenue FROM parity_d5 UNION ALL SELECT 'd6' AS q, cnt, revenue FROM parity_d6 UNION ALL SELECT 'd7' AS q, cnt, revenue FROM parity_d7 UNION ALL SELECT 'd8' AS q, cnt, revenue FROM parity_d8;
.output results/parity_depths.txt
SELECT CASE WHEN (SELECT count(DISTINCT cnt) FROM (SELECT cnt FROM parity_flat UNION ALL SELECT cnt FROM parity_d1 UNION ALL SELECT cnt FROM parity_d2 UNION ALL SELECT cnt FROM parity_d3 UNION ALL SELECT cnt FROM parity_d4 UNION ALL SELECT cnt FROM parity_d5 UNION ALL SELECT cnt FROM parity_d6 UNION ALL SELECT cnt FROM parity_d7 UNION ALL SELECT cnt FROM parity_d8)) = 1
  AND (SELECT count(DISTINCT revenue) FROM (SELECT revenue FROM parity_flat UNION ALL SELECT revenue FROM parity_d1 UNION ALL SELECT revenue FROM parity_d2 UNION ALL SELECT revenue FROM parity_d3 UNION ALL SELECT revenue FROM parity_d4 UNION ALL SELECT revenue FROM parity_d5 UNION ALL SELECT revenue FROM parity_d6 UNION ALL SELECT revenue FROM parity_d7 UNION ALL SELECT revenue FROM parity_d8)) = 1
  THEN 'PARITY_OK_ALL_DEPTHS' ELSE 'PARITY_FAIL' END AS verdict;
.output
