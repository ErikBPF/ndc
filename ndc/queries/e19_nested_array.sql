WITH selected AS (
 SELECT filter(lineitems, x -> x.l_quantity IN (5,6,7,8) AND x.l_shipmode IN ('AIR','RAIL')
 AND x.l_discount BETWEEN 0.05 AND 0.08) AS xs FROM orders_nested_v2)
SELECT coalesce(sum(size(xs)),0) AS n,
 sum(CASE WHEN size(xs)>0 THEN aggregate(xs, CAST(0 AS DECIMAL(38,4)),
 (acc,x) -> acc + CAST(x.l_extendedprice*(1-x.l_discount) AS DECIMAL(38,4))) END) AS rev
FROM selected
