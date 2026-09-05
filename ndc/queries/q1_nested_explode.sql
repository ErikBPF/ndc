SELECT li.l_returnflag, li.l_linestatus,
       sum(li.l_quantity) AS sum_qty, sum(li.l_extendedprice) AS sum_base_price,
       sum(li.l_extendedprice * (1 - li.l_discount)) AS sum_disc_price,
       sum(li.l_extendedprice * (1 - li.l_discount) * (1 + li.l_tax)) AS sum_charge,
       count(*) AS cnt
FROM (SELECT explode(lineitems) AS li FROM orders_nested)
WHERE li.l_shipdate <= DATE '1998-09-02'
GROUP BY li.l_returnflag, li.l_linestatus ORDER BY li.l_returnflag, li.l_linestatus
