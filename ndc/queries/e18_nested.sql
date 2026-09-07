SELECT count(*) AS n, sum(top_sum) AS total FROM (
 SELECT aggregate(slice(reverse(array_sort(transform(lineitems, x -> x.l_extendedprice))),1,2),
  CAST(0 AS DECIMAL(38,2)), (acc,x) -> acc+x) AS top_sum FROM orders_nested
) t WHERE top_sum > 30000
