-- Export the 8 dbgen tables from the database to Parquet.
SET threads TO 4;
SET memory_limit='8GB';

COPY lineitem TO 'data/lineitem.parquet'  (FORMAT parquet);
COPY orders   TO 'data/orders.parquet'    (FORMAT parquet);
COPY part     TO 'data/part.parquet'      (FORMAT parquet);
COPY supplier TO 'data/supplier.parquet'  (FORMAT parquet);
COPY customer TO 'data/customer.parquet'  (FORMAT parquet);
COPY partsupp TO 'data/partsupp.parquet'  (FORMAT parquet);
COPY nation   TO 'data/nation.parquet'    (FORMAT parquet);
COPY region   TO 'data/region.parquet'    (FORMAT parquet);
