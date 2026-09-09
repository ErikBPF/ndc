SELECT o_orderkey, count(li.l_linenumber) AS item_count
FROM orders_nested LATERAL VIEW OUTER explode(lineitems) u AS li
GROUP BY o_orderkey ORDER BY o_orderkey;
