SELECT o_orderkey, count(li.l_linenumber) AS item_count
FROM orders_nested_v2 LATERAL VIEW OUTER explode(lineitems) u AS li
GROUP BY o_orderkey ORDER BY o_orderkey;
