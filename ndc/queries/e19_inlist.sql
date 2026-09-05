SELECT count(*) AS n, sum(l.l_extendedprice * (1 - l.l_discount)) AS rev FROM lineitem l WHERE l.l_quantity IN (5, 6, 7, 8) AND l.l_shipmode IN ('AIR', 'RAIL') AND l.l_discount BETWEEN 0.05 AND 0.08
