-- DuckDB parity for the adapted query set; no timing measurements.
-- Output: results/duckdb.json (answers), results/parity.txt (validity gate).
SET threads TO 4;
INSTALL parquet; LOAD parquet;

CREATE OR REPLACE TABLE lineitem AS FROM 'data/lineitem.parquet';
CREATE OR REPLACE TABLE orders_nested AS FROM 'data/orders_nested.parquet/*.parquet';

CREATE OR REPLACE TABLE q1_flat AS
SELECT l_returnflag, l_linestatus,
       sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price,
       sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
       count(*) AS cnt
FROM lineitem
WHERE l_shipdate <= DATE '1998-09-02'
GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;

CREATE OR REPLACE TABLE q1_nested AS
SELECT li.l_returnflag, li.l_linestatus,
       sum(li.l_quantity) AS sum_qty, sum(li.l_extendedprice) AS sum_base_price,
       sum(li.l_extendedprice * (1 - li.l_discount)) AS sum_disc_price,
       sum(li.l_extendedprice * (1 - li.l_discount) * (1 + li.l_tax)) AS sum_charge,
       count(*) AS cnt
FROM (SELECT unnest(lineitems, recursive := false) AS li FROM orders_nested)
WHERE li.l_shipdate <= DATE '1998-09-02'
GROUP BY li.l_returnflag, li.l_linestatus ORDER BY li.l_returnflag, li.l_linestatus;

CREATE OR REPLACE TABLE q6_flat AS
SELECT sum(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;

CREATE OR REPLACE TABLE q6_nested AS
SELECT sum(la) AS revenue FROM (
  SELECT list_aggregate(list_transform(
           list_filter(lineitems, x -> x.l_shipdate >= DATE '1994-01-01'
                 AND x.l_shipdate < DATE '1995-01-01'
                 AND x.l_discount BETWEEN 0.05 AND 0.07
                 AND x.l_quantity < 24),
           x -> x.l_extendedprice * x.l_discount), 'sum') AS la
  FROM orders_nested)
WHERE la IS NOT NULL;

.mode json
.output results/duckdb.json
SELECT 'q1_flat' AS q, sum_qty, sum_base_price, sum_disc_price, sum_charge, cnt FROM q1_flat
UNION ALL SELECT 'q1_nested', sum_qty, sum_base_price, sum_disc_price, sum_charge, cnt FROM q1_nested
UNION ALL SELECT 'q6_flat', revenue, NULL, NULL, NULL, NULL FROM q6_flat
UNION ALL SELECT 'q6_nested', revenue, NULL, NULL, NULL, NULL FROM q6_nested;
.output results/parity.txt
SELECT CASE WHEN (SELECT count(*) FROM q1_flat) = (SELECT count(*) FROM q1_nested)
        AND NOT EXISTS (SELECT 1 FROM q1_flat f FULL JOIN q1_nested n USING (l_returnflag, l_linestatus)
                        WHERE f.sum_qty IS DISTINCT FROM n.sum_qty
                           OR f.sum_base_price IS DISTINCT FROM n.sum_base_price
                           OR f.sum_disc_price IS DISTINCT FROM n.sum_disc_price
                           OR f.sum_charge IS DISTINCT FROM n.sum_charge
                           OR f.cnt IS DISTINCT FROM n.cnt)
        AND (SELECT revenue FROM q6_flat) IS NOT DISTINCT FROM (SELECT revenue FROM q6_nested)
       THEN 'PARITY_OK' ELSE 'PARITY_FAIL' END AS verdict;
.output
