import argparse
import glob
import json
import statistics
import time

from pyspark.sql import SparkSession


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--comet", action="store_true")
    p.add_argument("--data", default="data")
    p.add_argument("--out", required=True)
    p.add_argument("--queries", default="queries/manifest-full.json",
                   help="query manifest JSON {name: sql-file}")
    p.add_argument("--runs", type=int, default=3)
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

    with open(a.queries) as f:
        manifest = json.load(f)
    queries = sorted(manifest.items())  # (name, sql-file), deterministic order

    results = []
    parity = {}
    for name, qfile in queries:
        sql = open(qfile).read()
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
