-- Exact bag comparison preserves retained values, multiplicity and parent membership.
CREATE TEMP TABLE flat_leaves AS SELECT l_orderkey,l_linenumber,l_partkey,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipmode FROM lineitem;
CREATE TEMP TABLE nested_leaves AS SELECT o_orderkey,x.l_linenumber,x.l_partkey,x.l_quantity,x.l_extendedprice,x.l_discount,x.l_tax,x.l_returnflag,x.l_linestatus,x.l_shipdate,x.l_commitdate,x.l_receiptdate,x.l_shipmode FROM (SELECT o_orderkey,unnest(lineitems) x FROM read_parquet('data/orders_nested.parquet'));
SELECT CASE WHEN EXISTS (
 (SELECT * FROM flat_leaves EXCEPT ALL SELECT * FROM nested_leaves)
 UNION ALL (SELECT * FROM nested_leaves EXCEPT ALL SELECT * FROM flat_leaves)
) THEN error('NESTED_INVARIANT_FAIL: leaf composition') ELSE 'NESTED_INVARIANT_OK' END;
SELECT CASE WHEN (SELECT count(*) FROM orders) !=
 (SELECT count(*) FROM read_parquet('data/orders_nested.parquet'))
 THEN error('NESTED_INVARIANT_FAIL: parent cardinality') ELSE 'PARENT_INVARIANT_OK' END;
