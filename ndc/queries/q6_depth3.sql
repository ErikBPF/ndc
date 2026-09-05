SELECT sum(x.l_extendedprice * x.l_discount) AS revenue, count(*) AS cnt
FROM orders_depth3
LATERAL VIEW explode(c) lv0 AS t0
LATERAL VIEW explode(t0.inner.inner) lv1 AS x
WHERE x.l_shipdate >= DATE '1994-01-01' AND x.l_shipdate < DATE '1995-01-01'
  AND x.l_discount BETWEEN 0.05 AND 0.07 AND x.l_quantity < 24
