"""Build per-format table copies from the flat/nested parquet data.
usage: write_fmt.py --fmt iceberg|delta --data data [--warehouse DIR]
Tables: lineitem, orders_nested, orders_depth1..8."""
import argparse
import os

from pyspark.sql import SparkSession

p = argparse.ArgumentParser()
p.add_argument("--fmt", required=True, choices=["iceberg", "delta"])
p.add_argument("--data", default="data")
p.add_argument("--warehouse", default=None)
a = p.parse_args()

b = SparkSession.builder.appName("write-fmt").master("local[8]")
b = b.config("spark.driver.memory", "32g")
b = b.config("spark.sql.shuffle.partitions", "16")
b = b.config("spark.ui.enabled", "false")
if a.fmt == "iceberg":
    b = (b.config("spark.sql.catalog.local_tpch", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.local_tpch.warehouse", a.warehouse or os.path.abspath("data_iceberg_wh"))
         .config("spark.sql.catalog.local_tpch.type", "hadoop"))
spark = b.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

tables = ["lineitem", "orders_nested"] + [f"orders_depth{d}" for d in range(1, 9)]
if a.fmt == "delta":
    for t in tables:
        (spark.read.parquet(f"{a.data}/{t}.parquet")
         .write.format("delta").mode("overwrite").save(f"data_delta/{t}"))
        print(f"WROTE delta/{t}", flush=True)
else:
    for t in tables:
        (spark.read.parquet(f"{a.data}/{t}.parquet")
         .writeTo(f"local_tpch.{t}").createOrReplace())
        print(f"WROTE iceberg/{t}", flush=True)
print("WRITE_FMT_OK", flush=True)
