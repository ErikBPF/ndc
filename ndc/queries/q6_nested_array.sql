WITH selected AS (
 SELECT filter(lineitems, x -> x.l_shipdate >= DATE '1994-01-01'
   AND x.l_shipdate < DATE '1995-01-01' AND x.l_discount BETWEEN 0.05 AND 0.07
   AND x.l_quantity < 24) AS xs FROM orders_nested_v2)
SELECT sum(CASE WHEN size(xs) > 0 THEN aggregate(xs, CAST(0 AS DECIMAL(38,4)),
 (acc,x) -> acc + CAST(x.l_extendedprice*x.l_discount AS DECIMAL(38,4))) END) AS revenue,
 coalesce(sum(size(xs)), 0) AS cnt FROM selected
