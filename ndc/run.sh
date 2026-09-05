#!/usr/bin/env bash
# TPC-H-derived nested POC, stage driver. Run from its own workspace copy:
# ./run.sh <stage> [args]
# Profiles:
#   lite    — during-work smoke: tiny scale, 1 run, parquet, both engines, parity gate
#   full    — release/PR discipline: all formats x engines, 3 runs, drop-caches, monitor
#   bootstrap <sf> <name> — new sibling workspace for a scale point
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p data results

# D11/D12: bench profile (cluster + fs target). Unimplemented targets fail loud.
if [ -f bench.conf ]; then
  # shellcheck disable=SC1091
  source bench.conf
fi
CLUSTER=${CLUSTER:-local}
FS=${FS:-local-fs}
case "$CLUSTER" in
  local) : ;;
  *) echo "CLUSTER=$CLUSTER declared in bench.conf but not implemented (D11); use local" >&2; exit 2 ;;
esac
case "$FS" in
  local-fs) : ;;
  s3-cygnus) echo "FS=s3-cygnus declared in bench.conf but not implemented (D12); needs hadoop-aws jars + Vault creds" >&2; exit 2 ;;
  *) echo "unknown FS=$FS (D12)" >&2; exit 2 ;;
esac

SPARK41_BASE=${SPARK41_BASE:-$HOME/ndc-spark41}
SPARK_HOME=$SPARK41_BASE/spark-4.1.3-bin-hadoop3
JARDIR=$SPARK41_BASE/jars
COMET_JAR=${COMET_JAR:-$JARDIR/comet-spark-spark4.1_2.13-1.0.0.jar}
ICEBERG_JAR=$JARDIR/iceberg-spark-runtime-4.1_2.13-1.11.0.jar
# Delta 4.4.0 is binary-incompatible with Spark 4.1.3; 4.3.x verified.
DELTA_JAR=${DELTA_JAR:-$JARDIR/delta-spark_2.13-4.3.1.jar}
DELTA_STORAGE_JAR=${DELTA_STORAGE_JAR:-$JARDIR/delta-storage-4.3.1.jar}
DMEM=${SPARK_DRIVER_MEM:-32g}

engine_flags() { # $1 = vanilla|comet, $2 = fmt
  local jars="" confs="" app=""
  if [ "$1" = comet ]; then
    jars=$COMET_JAR
    confs="--conf spark.driver.extraClassPath=$COMET_JAR --conf spark.executor.extraClassPath=$COMET_JAR"
    app="--comet"
  fi
  case "$2" in
    iceberg)
      jars="$jars,$ICEBERG_JAR"
      confs="$confs --conf spark.sql.catalog.local_tpch=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.local_tpch.warehouse=$PWD/data_iceberg_wh --conf spark.sql.catalog.local_tpch.type=hadoop"
      ;;
    delta)
      jars="$jars,$DELTA_JAR,$DELTA_STORAGE_JAR"
      confs="$confs --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=io.delta.spark.DeltaCatalog"
      ;;
  esac
  jars=${jars#,}
  local jf=""
  [ -n "$jars" ] && jf="--jars $jars"
  printf '%s\n' "$jf $confs" "$app"
}

dropcaches() {
  if sudo -n sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null; then
    echo dropped
  else
    echo drop-failed
  fi
}

spark_run() { # $1 = engine ; $2 = fmt ; $3 = query manifest ; $4 = runs ; $5 = drop-caches yes|no
  local eng=$1 fmt=$2 q=${3:-} runs=${4:-3} drop=${5:-no}
  local out=results/spark_${eng}_${fmt}.json
  local mon=results/monitor_${eng}_${fmt}.csv
  mapfile -t _ef < <(engine_flags "$eng" "$fmt")
  local jvmflags=${_ef[0]-} appflags=${_ef[1]-}
  local qflag=""; [ -n "$q" ] && qflag="--queries $q"
  local dflag=""; [ "$drop" = yes ] && dflag="--drop-caches"
  nix shell nixpkgs#openjdk nixpkgs#python3 -c bash -c "
    export JAVA_HOME=\$(dirname \$(dirname \$(command -v java)))
    export SPARK_HOME=$SPARK_HOME
    export PATH=$SPARK_HOME/bin:\$PATH
    exec spark-submit --driver-memory $DMEM $jvmflags spark_poc.py $appflags --fmt $fmt --data data --out $out --runs $runs $qflag $dflag" \
    > "results/spark_${eng}_${fmt}.stdout" 2>&1 &
  local spid=$! rc=0
  nix shell nixpkgs#python3 -c python3 monitor.py "$spid" "$mon" 1 >/dev/null 2>&1 &
  wait "$spid" || rc=$?
  nix shell nixpkgs#python3 -c python3 merge_monitor.py "$out" "$mon" || true
  tail -1 "results/spark_${eng}_${fmt}.stdout"
  return $rc
}

matrix() { # $1 = runs ; $2 = drop-caches yes|no ; $3 = query manifest
  local runs=${1:-3} drop=${2:-yes} q=${3:-queries/manifest-depth.json}
  for fmt in parquet iceberg delta; do
    for eng in vanilla comet; do
      dropcaches >/dev/null
      if ! spark_run "$eng" "$fmt" "$q" "$runs" "$drop"; then
        echo "CELL_FAIL $eng/$fmt"
      fi
    done
  done
}

case "${1:-}" in
  gen)
    nix shell nixpkgs#duckdb -c bash -c 'duckdb tpch.duckdb < gen.sql && ls -la tpch.duckdb | awk "{print \$5, \$9}"'
    ;;
  conv)
    nix shell nixpkgs#duckdb -c bash -c 'duckdb tpch.duckdb < conv.sql && ls -la data/*.parquet | awk "{print \$5, \$9}"'
    ;;
  nested)
    nix shell nixpkgs#duckdb -c bash -c 'duckdb tpch.duckdb < nested.sql && ls -la data/orders_nested.parquet | awk "{print \$5, \$9}"'
    ;;
  parity)
    nix shell nixpkgs#duckdb -c bash -c 'duckdb < parity.sql && cat results/parity.txt'
    ;;
  depths)
    nix shell nixpkgs#duckdb -c bash -c 'duckdb tpch.duckdb < depths.sql && cat results/parity_depths.txt &&
      ls -la data/orders_depth*.parquet | awk "{print \$5, \$9}"'
    ;;
  setup)
    mkdir -p "$SPARK41_BASE" "$JARDIR"
    if [ ! -d "$SPARK_HOME" ]; then
      curl -sS -o "$SPARK41_BASE/spark.tgz" https://archive.apache.org/dist/spark/spark-4.1.3/spark-4.1.3-bin-hadoop3.tgz
      tar xzf "$SPARK41_BASE/spark.tgz" -C "$SPARK41_BASE"
      rm "$SPARK41_BASE/spark.tgz"
    fi
    for pair in \
      "$COMET_JAR|https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark4.1_2.13/1.0.0/comet-spark-spark4.1_2.13-1.0.0.jar" \
      "$ICEBERG_JAR|https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar" \
      "$DELTA_JAR|https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.3.1/delta-spark_2.13-4.3.1.jar" \
      "$DELTA_STORAGE_JAR|https://repo1.maven.org/maven2/io/delta/delta-storage/4.3.1/delta-storage-4.3.1.jar"; do
      f=${pair%%|*}; url=${pair#*|}
      [ -s "$f" ] || curl -sS -o "$f" -w "%{http_code} $(basename "$f") %{size_download}\n" "$url"
    done
    ls -la "$JARDIR" | awk '{print $5, $9}'
    ;;
  build-fmt)
    # $2 = iceberg|delta
    mapfile -t _ef < <(engine_flags vanilla "$2")
    jvmflags=${_ef[0]-} appflags=${_ef[1]-}
    nix shell nixpkgs#openjdk nixpkgs#python3 -c bash -c "
      export JAVA_HOME=\$(dirname \$(dirname \$(command -v java)))
      export SPARK_HOME=$SPARK_HOME
      export PATH=$SPARK_HOME/bin:\$PATH
      spark-submit --driver-memory $DMEM $jvmflags write_fmt.py --fmt $2 --data data" \
      > "results/buildfmt_$2.stdout" 2>&1
    tail -2 "results/buildfmt_$2.stdout"
    ;;
  spark)
    # $2 = vanilla|comet ; $3 = fmt ; $4 = query manifest ; $5 = runs ; $6 = drop-caches yes|no
    spark_run "$2" "${3:-parquet}" "${4:-}" "${5:-3}" "${6:-no}"
    ;;
  bootstrap)
    # $2 = sf ; $3 = name -> creates a new sibling scale workspace under WS_ROOT.
    # Copies only scripts (never data/results); writes gen.sql with the SF knob.
    local_name=$3
    ws=${WS_ROOT:-$HOME/ndc-workspaces}/tpch-$local_name
    mkdir -p "$ws/data" "$ws/results"
    for f in run.sh spark_poc.py conv.sql nested.sql parity.sql depths.sql monitor.py merge_monitor.py write_fmt.py check_size.py report.py sizes.csv; do
      cp "$f" "$ws/"
    done
    cp -r "$PWD/queries" "$ws/queries"
    printf "SET threads TO 8;\nSET memory_limit='24GB';\nINSTALL tpch; LOAD tpch;\nCALL dbgen(sf = %s);\n" "$2" > "$ws/gen.sql"
    chmod +x "$ws/run.sh"
    echo "BOOTSTRAPPED $ws (sf=$2)"
    ;;
  build-scale)
    # full data build in this workspace: gen -> conv -> nested -> depths -> formats
    ./run.sh gen && ./run.sh conv && ./run.sh nested && ./run.sh depths
    ./run.sh build-fmt iceberg && ./run.sh build-fmt delta
    ;;
  lite)
    # during-work smoke: data must exist; 1 run; parquet only; both engines
    ./run.sh depthsmoke
    for eng in vanilla comet; do
      spark_run "$eng" parquet "${QUERIES:-queries/manifest-depth.json}" 1 no
    done
    ;;
  full)
    # release discipline: all formats x engines, 3 runs, drop-caches, monitor
    matrix 3 yes "${QUERIES:-queries/manifest-full.json}"
    ;;
  comet-default)
    # D13: the default tpch-ndc kit for our DataFusion Comet work
    matrix 3 yes "${QUERIES:-queries/manifest-full.json}"
    nix shell nixpkgs#python3 -c python3 report.py results results/report.md || true
    ;;
  report)
    nix shell nixpkgs#python3 -c python3 report.py results results/report.md || true
    ;;
  size-check)
    # D10: assert per-SF row counts against sizes.csv (sf parsed from gen.sql)
    nix shell nixpkgs#duckdb -c bash -c 'duckdb -csv tpch.duckdb -c "
SELECT (SELECT count(*) FROM lineitem) AS lineitem, (SELECT count(*) FROM orders) AS orders,
       (SELECT count(*) FROM part) AS part, (SELECT count(*) FROM customer) AS customer,
       (SELECT count(*) FROM supplier) AS supplier, (SELECT count(*) FROM partsupp) AS partsupp;"' | tee results/size-check.csv
    nix shell nixpkgs#python3 -c python3 check_size.py gen.sql results/size-check.csv sizes.csv
    ;;
  depthsmoke)
    nix shell nixpkgs#duckdb -c bash -c 'duckdb tpch.duckdb < depths.sql && cat results/parity_depths.txt' | tail -1
    ;;
  *) echo "usage: run.sh gen|conv|nested|parity|depths|setup|bootstrap <sf> <name>|build-scale|build-fmt <fmt>|spark <eng> <fmt> [manifest] [runs] [drop]|lite|full|comet-default|report|size-check|depthsmoke"; exit 1;;
esac
