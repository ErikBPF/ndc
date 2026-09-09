SELECT o_orderkey, count(*) AS item_count,
       sum(li.l_extendedprice * li.l_discount) AS revenue
FROM orders_nested CROSS JOIN UNNEST(lineitems) AS u(li)
GROUP BY o_orderkey ORDER BY o_orderkey;
