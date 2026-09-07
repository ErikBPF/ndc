-- Schema v2; nested non-null and key invariants are validated by datagen.
CREATE TABLE orders_nested_v2 (
    o_orderkey BIGINT NOT NULL,
    o_custkey BIGINT NOT NULL,
    o_orderstatus VARCHAR NOT NULL,
    o_totalprice DECIMAL(15,2) NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority VARCHAR NOT NULL,
    o_clerk VARCHAR NOT NULL,
    o_shippriority INTEGER NOT NULL,
    o_comment VARCHAR NOT NULL,
    lineitems ARRAY(OBJECT(
        "l_partkey" BIGINT,
        "l_suppkey" BIGINT,
        "l_linenumber" INTEGER,
        "l_quantity" DECIMAL(15,2),
        "l_extendedprice" DECIMAL(15,2),
        "l_discount" DECIMAL(15,2),
        "l_tax" DECIMAL(15,2),
        "l_returnflag" VARCHAR,
        "l_linestatus" VARCHAR,
        "l_shipdate" DATE,
        "l_commitdate" DATE,
        "l_receiptdate" DATE,
        "l_shipinstruct" VARCHAR,
        "l_shipmode" VARCHAR,
        "l_comment" VARCHAR
    ) NOT NULL) NOT NULL
);
