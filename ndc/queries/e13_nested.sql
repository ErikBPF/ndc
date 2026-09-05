SELECT coalesce(c.c_mktsegment, 'NONE') AS seg, count(*) AS n_orders FROM orders_nested o LEFT JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY 1 ORDER BY 1
