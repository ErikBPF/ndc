SELECT count(*) AS n, sum(top_sum) AS total FROM (
 SELECT l_orderkey, sum(l_extendedprice) AS top_sum FROM (
  SELECT l_orderkey, l_extendedprice, row_number() OVER (
   PARTITION BY l_orderkey ORDER BY l_extendedprice DESC, l_linenumber) AS rn FROM lineitem
 ) t WHERE rn <= 2 GROUP BY l_orderkey
) t WHERE top_sum > 30000
