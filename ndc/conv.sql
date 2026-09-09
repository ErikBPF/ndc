-- Export the 8 dbgen tables from the database to Parquet.
SET threads TO 4;

COPY lineitem TO 'source/lineitem.parquet'  (FORMAT parquet);
COPY orders   TO 'source/orders.parquet'    (FORMAT parquet);
COPY part     TO 'source/part.parquet'      (FORMAT parquet);
COPY supplier TO 'source/supplier.parquet'  (FORMAT parquet);
COPY customer TO 'source/customer.parquet'  (FORMAT parquet);
COPY partsupp TO 'source/partsupp.parquet'  (FORMAT parquet);
COPY nation   TO 'source/nation.parquet'    (FORMAT parquet);
COPY region   TO 'source/region.parquet'    (FORMAT parquet);
