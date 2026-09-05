SELECT upper(substring(p.p_brand, 1, 5)) AS b, count(*) AS n FROM lineitem l JOIN part p ON l.l_partkey = p.p_partkey WHERE l.l_shipmode = 'TRUCK' GROUP BY 1 ORDER BY 1
