-- Schema v2; nested non-null and key invariants are validated by datagen.
CREATE TABLE orders_nested_v2 (
    o_orderkey BIGINT NOT NULL,
    o_custkey BIGINT NOT NULL,
    o_orderstatus STRING NOT NULL,
    o_totalprice DECIMAL(15,2) NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority STRING NOT NULL,
    o_clerk STRING NOT NULL,
    o_shippriority INTEGER NOT NULL,
    o_comment STRING NOT NULL,
    lineitems ARRAY<STRUCT<
        l_partkey:BIGINT,
        l_suppkey:BIGINT,
        l_linenumber:INTEGER,
        l_quantity:DECIMAL(15,2),
        l_extendedprice:DECIMAL(15,2),
        l_discount:DECIMAL(15,2),
        l_tax:DECIMAL(15,2),
        l_returnflag:STRING,
        l_linestatus:STRING,
        l_shipdate:DATE,
        l_commitdate:DATE,
        l_receiptdate:DATE,
        l_shipinstruct:STRING,
        l_shipmode:STRING,
        l_comment:STRING
    >> NOT NULL
) USING PARQUET;
