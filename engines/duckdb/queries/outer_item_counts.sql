SELECT o_orderkey, count(li.l_linenumber) AS item_count
FROM orders_nested_v2 LEFT JOIN LATERAL UNNEST(lineitems) AS u(li) ON true
GROUP BY o_orderkey ORDER BY o_orderkey;
