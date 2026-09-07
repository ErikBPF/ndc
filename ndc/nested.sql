-- Nested transform: ORDERS + lineitem struct-array. Parity invariant: per-order
-- composition preserved; no line item added, dropped, or altered.
SET threads TO 4;
SET memory_limit='8GB';

COPY (
  SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
         o_orderpriority, o_clerk, o_shippriority,
         list(struct_pack(
           l_linenumber := l.l_linenumber,
           l_partkey := l.l_partkey,
           l_quantity := l.l_quantity,
           l_extendedprice := l.l_extendedprice,
           l_discount := l.l_discount,
           l_tax := l.l_tax,
           l_returnflag := l.l_returnflag,
           l_linestatus := l.l_linestatus,
           l_shipdate := l.l_shipdate,
           l_commitdate := l.l_commitdate,
           l_receiptdate := l.l_receiptdate,
           l_shipmode := l.l_shipmode) ORDER BY l.l_linenumber) AS lineitems
  FROM orders o JOIN lineitem l ON l.l_orderkey = o.o_orderkey
  GROUP BY ALL
  ORDER BY o_orderkey
) TO 'data/orders_nested.parquet' (FORMAT parquet);
