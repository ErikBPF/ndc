import argparse
import glob
import json
import statistics
import time

from pyspark.sql import SparkSession

Q1_FLAT = """
SELECT l_returnflag, l_linestatus,
       sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price,
       sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
       count(*) AS cnt
FROM lineitem
WHERE l_shipdate <= DATE '1998-09-02'
GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus
"""

Q1_NESTED_EXPLODE = """
SELECT li.l_returnflag, li.l_linestatus,
       sum(li.l_quantity) AS sum_qty, sum(li.l_extendedprice) AS sum_base_price,
       sum(li.l_extendedprice * (1 - li.l_discount)) AS sum_disc_price,
       sum(li.l_extendedprice * (1 - li.l_discount) * (1 + li.l_tax)) AS sum_charge,
       count(*) AS cnt
FROM (SELECT explode(lineitems) AS li FROM orders_nested)
WHERE li.l_shipdate <= DATE '1998-09-02'
GROUP BY li.l_returnflag, li.l_linestatus ORDER BY li.l_returnflag, li.l_linestatus
"""

Q6_FLAT = """
SELECT sum(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24
"""

Q6_NESTED_ARRAY = """
SELECT sum(agg) AS revenue
FROM (SELECT aggregate(
        filter(lineitems, x -> x.l_shipdate >= DATE '1994-01-01'
              AND x.l_shipdate < DATE '1995-01-01'
              AND x.l_discount BETWEEN 0.05 AND 0.07
              AND x.l_quantity < 24),
        CAST(0 AS DECIMAL(38, 4)),
        (acc, x) -> acc + CAST(x.l_extendedprice * x.l_discount AS DECIMAL(38, 4))) AS agg
      FROM orders_nested)
"""

QUERIES = [
    ("q1_flat", Q1_FLAT),
    ("q1_nested_explode", Q1_NESTED_EXPLODE),
    ("q6_flat", Q6_FLAT),
    ("q6_nested_array", Q6_NESTED_ARRAY),
]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--comet", action="store_true")
    p.add_argument("--data", default="data")
    p.add_argument("--out", required=True)
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--queries", default=None, help="JSON file {name: sql} merged into the built-in set")
    p.add_argument("--fmt", default="parquet", choices=["parquet", "iceberg", "delta"])
    p.add_argument("--drop-caches", action="store_true")
    a = p.parse_args()

    b = SparkSession.builder.appName("tpch-nested-poc").master("local[8]")
    b = b.config("spark.driver.memory", "32g")
    b = b.config("spark.sql.shuffle.partitions", "16")
    b = b.config("spark.ui.enabled", "false")
    b = b.config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    if a.comet:
        jar = "comet-spark-spark4.0_2.13-1.0.0.jar"
        b = (b.config("spark.jars", jar)
             .config("spark.plugins", "org.apache.spark.CometPlugin")
             .config("spark.shuffle.manager",
                     "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
             .config("spark.comet.explain.fallback.enabled", "true")
             .config("spark.memory.offHeap.enabled", "true")
             .config("spark.memory.offHeap.size", "4g"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for t in ["lineitem", "orders", "customer", "part"]:
        spark.read.parquet(f"{a.data}/{t}.parquet").createOrReplaceTempView(t)
    if a.fmt == "parquet":
        spark.read.parquet(f"{a.data}/orders_nested.parquet").createOrReplaceTempView("orders_nested")
        import glob
        for f in sorted(glob.glob(f"{a.data}/orders_depth*.parquet")):
            name = f.rsplit("/", 1)[1].replace(".parquet", "")
            spark.read.parquet(f).createOrReplaceTempView(name)
    elif a.fmt == "iceberg":
        for t in ["orders_nested"] + [f"orders_depth{d}" for d in range(1, 9)]:
            spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {t} AS SELECT * FROM local_tpch.{t}")
    elif a.fmt == "delta":
        for t in ["orders_nested"] + [f"orders_depth{d}" for d in range(1, 9)]:
            spark.read.format("delta").load(f"data_delta/{t}").createOrReplaceTempView(t)

    import time as _t
    t_start = _t.time()

    queries = list(QUERIES)
    if a.queries:
        with open(a.queries) as f:
            queries = queries + sorted(json.load(f).items())

    results = []
    parity = {}
    for name, sql in queries:
        rows = None
        for i in range(a.runs):
            cache = "warm"
            if a.drop_caches:
                import subprocess
                cp = subprocess.run(["sudo", "-n", "sh", "-c",
                                     "sync; echo 3 > /proc/sys/vm/drop_caches"],
                                    capture_output=True)
                cache = "cold" if cp.returncode == 0 else "drop-failed"
            t0 = time.time()
            print(f"MARK {name} run {i} {t0:.3f} {cache}", flush=True)
            c0 = time.perf_counter()
            rows = spark.sql(sql).collect()
            ms = (time.perf_counter() - c0) * 1000.0
            results.append({"q": name, "run": i, "ms": ms, "t0": t0,
                            "t1": time.time(), "cache": cache,
                            "engine": ("spark+comet" if a.comet else "spark") + f"/{a.fmt}"})
        parity[name] = [tuple(r) for r in rows]

    ok = (parity["q1_flat"] == parity["q1_nested_explode"]
          and round(parity["q6_flat"][0][0], 2) == round(parity["q6_nested_array"][0][0], 2))
    if "q6_depth8" in parity:
        flat6 = round(parity["q6_flat"][0][0], 2)
        ok = ok and all(round(parity[f"q6_depth{d}"][0][0], 2) == flat6 for d in range(1, 9))
    with open(a.out, "w") as f:
        json.dump({"results": results,
                   "parity_ok": ok,
                   "fmt": a.fmt,
                   "spark_version": spark.version,
                   "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                   "answers": {k: [[str(c) for c in r] for r in v] for k, v in parity.items()}},
                  f, indent=1)
    print(f"PARITY {'OK' if ok else 'FAIL'} -> {a.out}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
