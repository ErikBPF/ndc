-- tpch-ndc extended pair parity (DuckDB, tiny workspace, in-db dbgen tables).
-- Six flat/nested pairs; nested side reads data/orders_nested.parquet.
-- Verdict: PARITY per pair (flat answers == nested answers, DuckDB anchors).
SET threads TO 8;
SET memory_limit='16GB';

CREATE OR REPLACE TABLE nest AS SELECT * FROM read_parquet('data/orders_nested.parquet');

-- E13: join + coalesce + null (Q13 spirit, segment-level)
CREATE OR REPLACE TABLE e13_flat AS
SELECT coalesce(c.c_mktsegment, 'NONE') AS seg, count(*) AS n_orders
FROM orders o LEFT JOIN customer c ON o.o_custkey = c.c_custkey
GROUP BY 1 ORDER BY 1;
CREATE OR REPLACE TABLE e13_nested AS
SELECT coalesce(c.c_mktsegment, 'NONE') AS seg, count(*) AS n_orders
FROM nest o LEFT JOIN customer c ON o.o_custkey = c.c_custkey
GROUP BY 1 ORDER BY 1;

-- E14: CASE + join (Q14 spirit, promo revenue share)
CREATE OR REPLACE TABLE e14_flat AS
SELECT count(*) AS n,
       sum(CASE WHEN p.p_type LIKE 'PROMO%' THEN l.l_extendedprice * (1 - l.l_discount) ELSE 0 END) AS promo_rev,
       sum(l.l_extendedprice * (1 - l.l_discount)) AS total_rev
FROM lineitem l JOIN part p ON l.l_partkey = p.p_partkey;
CREATE OR REPLACE TABLE e14_nested AS
SELECT count(*) AS n,
       sum(CASE WHEN p.p_type LIKE 'PROMO%' THEN li.l_extendedprice * (1 - li.l_discount) ELSE 0 END) AS promo_rev,
       sum(li.l_extendedprice * (1 - li.l_discount)) AS total_rev
FROM (SELECT unnest(lineitems) AS li FROM nest) t
JOIN part p ON li.l_partkey = p.p_partkey;

-- E19: IN-lists + arithmetic; nested side is a pure HOF predicate (no explode)
CREATE OR REPLACE TABLE e19_flat AS
SELECT count(*) AS n, sum(l.l_extendedprice * (1 - l.l_discount)) AS rev
FROM lineitem l
WHERE l.l_quantity IN (5, 6, 7, 8) AND l.l_shipmode IN ('AIR', 'RAIL')
  AND l.l_discount BETWEEN 0.05 AND 0.08;
CREATE OR REPLACE TABLE e19_nested AS
SELECT sum(length(filter(lineitems, x -> x.l_quantity IN (5, 6, 7, 8)
        AND x.l_shipmode IN ('AIR', 'RAIL') AND x.l_discount BETWEEN 0.05 AND 0.08))) AS n,
       sum(list_aggregate(list_transform(
             filter(lineitems, x -> x.l_quantity IN (5, 6, 7, 8)
                 AND x.l_shipmode IN ('AIR', 'RAIL')
                 AND x.l_discount BETWEEN 0.05 AND 0.08),
             x -> x.l_extendedprice * (1 - x.l_discount)), 'sum')) AS rev
FROM nest;

-- E07: string manipulation + join (Q7 spirit)
CREATE OR REPLACE TABLE e07_flat AS
SELECT upper(substring(p.p_brand, 1, 5)) AS b, count(*) AS n
FROM lineitem l JOIN part p ON l.l_partkey = p.p_partkey
WHERE l.l_shipmode = 'TRUCK'
GROUP BY 1 ORDER BY 1;
CREATE OR REPLACE TABLE e07_nested AS
SELECT upper(substring(p.p_brand, 1, 5)) AS b, count(*) AS n
FROM (SELECT unnest(lineitems) AS li FROM nest) t
JOIN part p ON li.l_partkey = p.p_partkey
WHERE li.l_shipmode = 'TRUCK'
GROUP BY 1 ORDER BY 1;

-- E21: correlated quantifiers (Q21 spirit) -> pure nested boolean quantifiers
CREATE OR REPLACE TABLE e21_flat AS
SELECT count(*) AS n FROM orders o
WHERE o.o_orderstatus = 'F'
  AND EXISTS (SELECT 1 FROM lineitem l WHERE l.l_orderkey = o.o_orderkey
              AND l.l_receiptdate > l.l_commitdate)
  AND NOT EXISTS (SELECT 1 FROM lineitem l2 WHERE l2.l_orderkey = o.o_orderkey
                  AND l2.l_shipdate > l2.l_commitdate);
CREATE OR REPLACE TABLE e21_nested AS
SELECT count(*) AS n FROM nest
WHERE o_orderstatus = 'F'
  AND length(list_filter(lineitems, x -> x.l_receiptdate > x.l_commitdate)) > 0
  AND length(list_filter(lineitems, x -> x.l_shipdate > x.l_commitdate)) = 0;

-- E18: top-N selection (window vs list ops)
CREATE OR REPLACE TABLE e18_flat AS
SELECT count(*) AS n FROM (
  SELECT l_orderkey FROM (
    SELECT l_orderkey, row_number() OVER (PARTITION BY l_orderkey
      ORDER BY l_extendedprice DESC) AS rn, l_extendedprice FROM lineitem)
  WHERE rn <= 2 AND l_extendedprice > 30000
  GROUP BY l_orderkey);
CREATE OR REPLACE TABLE e18_nested AS
SELECT count(*) AS n FROM (
  SELECT list_reverse(list_sort(list_transform(lineitems, x -> x.l_extendedprice))) AS tops
  FROM nest) t
WHERE length(list_slice(tops, 1, 2)) > 0
  AND length(list_filter(list_slice(tops, 1, 2), e -> e > 30000)) > 0;

.mode json
.output results/ext_parity.json
SELECT 'e13' AS pair, count(*) AS rows FROM e13_flat WHERE false
UNION ALL SELECT 'e13', 0;
.output results/ext_verdict.txt
SELECT
 CASE WHEN (SELECT count(*) FROM e13_flat) = (SELECT count(*) FROM e13_nested)
   AND NOT EXISTS (SELECT 1 FROM e13_flat f FULL JOIN e13_nested n USING (seg) WHERE f.n_orders IS DISTINCT FROM n.n_orders)
 AND (SELECT n FROM e14_flat) IS NOT DISTINCT FROM (SELECT n FROM e14_nested)
   AND (SELECT promo_rev FROM e14_flat) IS NOT DISTINCT FROM (SELECT promo_rev FROM e14_nested)
   AND (SELECT total_rev FROM e14_flat) IS NOT DISTINCT FROM (SELECT total_rev FROM e14_nested)
 AND (SELECT n FROM e19_flat) IS NOT DISTINCT FROM (SELECT n FROM e19_nested)
   AND (SELECT rev FROM e19_flat) IS NOT DISTINCT FROM (SELECT rev FROM e19_nested)
 AND (SELECT count(*) FROM e07_flat) = (SELECT count(*) FROM e07_nested)
   AND NOT EXISTS (SELECT 1 FROM e07_flat f FULL JOIN e07_nested n USING (b) WHERE f.n IS DISTINCT FROM n.n)
 AND (SELECT n FROM e21_flat) IS NOT DISTINCT FROM (SELECT n FROM e21_nested)
 AND (SELECT n FROM e18_flat) IS NOT DISTINCT FROM (SELECT n FROM e18_nested)
 THEN 'EXT_PARITY_OK' ELSE 'EXT_PARITY_FAIL' END AS verdict;
.output
