SELECT o_orderkey, count(*) AS item_count,
       sum(li.l_extendedprice * li.l_discount) AS revenue
FROM orders_nested_v2 LATERAL VIEW explode(lineitems) u AS li
GROUP BY o_orderkey ORDER BY o_orderkey;
