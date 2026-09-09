SELECT o_orderkey, count(li.value:l_linenumber) AS item_count
FROM orders_nested, LATERAL FLATTEN(INPUT => lineitems, OUTER => TRUE) li
GROUP BY o_orderkey ORDER BY o_orderkey;
