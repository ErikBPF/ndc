SELECT l_returnflag, l_linestatus,
       sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price,
       sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
       count(*) AS cnt
FROM lineitem
WHERE l_shipdate <= DATE '1998-09-02'
GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus
