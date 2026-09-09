SELECT o_orderkey, count(*) AS item_count,
       sum(li.value:l_extendedprice::NUMBER(15,2) *
           li.value:l_discount::NUMBER(15,2)) AS revenue
FROM orders_nested, LATERAL FLATTEN(INPUT => lineitems) li
GROUP BY o_orderkey ORDER BY o_orderkey;
