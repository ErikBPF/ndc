#!/usr/bin/env bash
# One pinned checkout; datasets and immutable campaigns live in an explicit workspace.
set -euo pipefail
CODE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(dirname "$CODE")
if [[ ${NDC_IN_ENV:-} != 1 ]]; then
  exec nix develop "path:$ROOT/nix" -c env NDC_IN_ENV=1 bash "$0" "$@"
fi
export NDC_WORKSPACE=${NDC_WORKSPACE:-$ROOT/workspaces/default}
mkdir -p "$NDC_WORKSPACE"
cd "$NDC_WORKSPACE"
if [[ -f bench.conf ]]; then
  # shellcheck disable=SC1091
  source bench.conf
fi
[[ ${CLUSTER:-local} == local && ${FS:-local-fs} == local-fs ]] || { echo 'Only local/local-fs implemented' >&2; exit 2; }
SPARK41_BASE=${SPARK41_BASE:-$HOME/ndc-spark41}
export SPARK_HOME=${SPARK_HOME:-$SPARK41_BASE/spark-4.1.3-bin-hadoop3}
export PATH="$SPARK_HOME/bin:$PATH"
JARDIR=$SPARK41_BASE/jars
COMET_JAR=${COMET_JAR:-$JARDIR/comet-spark-spark4.1_2.13-1.0.0.jar}
ICEBERG_JAR=${ICEBERG_JAR:-$JARDIR/iceberg-spark-runtime-4.1_2.13-1.11.0.jar}
DELTA_JAR=${DELTA_JAR:-$JARDIR/delta-spark_2.13-4.3.1.jar}
DELTA_STORAGE_JAR=${DELTA_STORAGE_JAR:-$JARDIR/delta-storage-4.3.1.jar}
MASTER=${SPARK_MASTER:-local[4]}
DMEM=${SPARK_DRIVER_MEM:-8g}

engine_flags() {
  local engine=$1 fmt=$2
  JVM_FLAGS=(--master "$MASTER" --driver-memory "$DMEM")
  JARS=()
  [[ $engine == vanilla || $engine == comet ]] || return 2
  if [[ $engine == comet ]]; then
    JARS+=("$COMET_JAR")
    JVM_FLAGS+=(--conf "spark.driver.extraClassPath=$COMET_JAR" --conf "spark.executor.extraClassPath=$COMET_JAR")
  fi
  case "$fmt" in
    parquet) ;;
    iceberg)
      JARS+=("$ICEBERG_JAR")
      JVM_FLAGS+=(--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
        --conf spark.sql.catalog.local_tpch=org.apache.iceberg.spark.SparkCatalog
        --conf "spark.sql.catalog.local_tpch.warehouse=$PWD/data_iceberg_wh"
        --conf spark.sql.catalog.local_tpch.type=hadoop) ;;
    delta)
      JARS+=("$DELTA_JAR" "$DELTA_STORAGE_JAR")
      JVM_FLAGS+=(--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog) ;;
    *) echo "Unknown format: $fmt" >&2; return 2 ;;
  esac
  NDC_JARS=$(IFS=,; echo "${JARS[*]}")
  export NDC_JARS
  [[ ${#JARS[@]} == 0 ]] || JVM_FLAGS+=(--jars "$NDC_JARS")
}

spark_run() {
  local eng=$1 fmt=$2 q=${3:-$CODE/queries/manifest-full.json} runs=${4:-3} drop=${5:-no}
  local campaign=${NDC_CAMPAIGN_DIR:-$PWD/results/$(date -u +%Y%m%dT%H%M%S)-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')}
  mkdir -p "$campaign"
  local out=$campaign/spark_${eng}_${fmt}.json mon=$campaign/monitor_${eng}_${fmt}.csv
  [[ ! -e $out ]] || { echo "Refusing existing result $out" >&2; return 2; }
  engine_flags "$eng" "$fmt"
  local args=(--fmt "$fmt" --data "$PWD/data" --out "$out" --queries "$q" --runs "$runs"
    --sf "$(cat scale.txt)" --campaign-id "$(basename "$campaign")" --streams "${STREAMS:-1}"
    --warmups "${WARMUPS:-1}" --seed "${SEED:-7}" --cache "${CACHE:-uncontrolled}"
    --layout-mode "${LAYOUT_MODE:-matched}" --validation "${VALIDATION:-collect}")
  [[ $eng != comet ]] || args+=(--comet)
  [[ $drop != yes ]] || args+=(--drop-caches)
  spark-submit "${JVM_FLAGS[@]}" "$CODE/spark_poc.py" "${args[@]}" > "$campaign/spark_${eng}_${fmt}.stdout" 2>&1 &
  local spid=$! rc=0
  python3 "$CODE/monitor.py" "$spid" "$mon" "${MONITOR_INTERVAL:-0.2}" &
  local mpid=$!
  wait "$spid" || rc=$?
  wait "$mpid" || rc=1
  if [[ -f $out ]]; then
    python3 "$CODE/merge_monitor.py" "$out" "$mon" || rc=1
  fi
  tail -3 "$campaign/spark_${eng}_${fmt}.stdout"
  return "$rc"
}

matrix() { # Continue for diagnostics, but any failed cell fails the campaign.
  local runs=${1:-3} drop=${2:-no} q=${3:-$CODE/queries/manifest-all.json} failed=0
  local formats=${FORMATS:-parquet iceberg delta} engines=${ENGINES:-vanilla comet}
  local base=${NDC_WORKSPACE:-$PWD}
  export NDC_CAMPAIGN_DIR=${NDC_CAMPAIGN_DIR:-$base/results/$(date -u +%Y%m%dT%H%M%S)-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')}
  [[ ! -e $NDC_CAMPAIGN_DIR ]] || { echo "Campaign already exists: $NDC_CAMPAIGN_DIR" >&2; return 2; }
  mkdir -p "$NDC_CAMPAIGN_DIR"
  local cells=() fmt eng
  # Engines alternate across format cells; repetitions use the same seeded query permutations.
  for fmt in $formats; do
    for eng in $engines; do
      cells+=("$eng/$fmt")
      if ! spark_run "$eng" "$fmt" "$q" "$runs" "$drop"; then
        echo "CELL_FAIL $eng/$fmt" >&2
        failed=1
      fi
    done
    if [[ $engines == 'vanilla comet' ]]; then engines='comet vanilla'; else engines=${ENGINES:-vanilla comet}; fi
  done
  python3 - "$NDC_CAMPAIGN_DIR/campaign.json" "$failed" "${cells[@]}" <<'PY'
import json,sys
from pathlib import Path
Path(sys.argv[1]).write_text(json.dumps({'status':'failed' if int(sys.argv[2]) else 'ok','cells':sys.argv[3:]},indent=2))
PY
  if [[ $failed == 0 ]]; then python3 "$CODE/report.py" "$NDC_CAMPAIGN_DIR" "$NDC_CAMPAIGN_DIR/report.md" || failed=1; fi
  echo "CAMPAIGN -> $NDC_CAMPAIGN_DIR"
  return "$failed"
}

case "${1:-}" in
  setup) python3 "$CODE/setup.py" "$SPARK41_BASE" ;;
  bootstrap)
    sf=${2:?scale factor required}; name=${3:?workspace name required}
    [[ $name =~ ^[A-Za-z0-9_-]+$ && $sf =~ ^[0-9]+([.][0-9]+)?$ ]] || exit 2
    ws=${WS_ROOT:-$ROOT/workspaces}/tpch-$name
    [[ ! -e $ws ]] || { echo "Workspace already exists: $ws" >&2; exit 2; }
    mkdir -p "$ws"
    printf '%s\n' "$sf" > "$ws/scale.txt"
    printf "SET threads TO 4;\nSET memory_limit='8GB';\nINSTALL tpch; LOAD tpch;\nCALL dbgen(sf = %s);\n" "$sf" > "$ws/gen.sql"
    cp "$CODE/bench.conf" "$ws/bench.conf"
    printf '#!/usr/bin/env bash\nexport NDC_WORKSPACE=%q\nexec %q "$@"\n' "$ws" "$CODE/run.sh" > "$ws/run.sh"
    chmod +x "$ws/run.sh"
    echo "BOOTSTRAPPED $ws" ;;
  gen) mkdir -p data results; duckdb tpch.duckdb < gen.sql ;;
  conv|nested) duckdb tpch.duckdb < "$CODE/$1.sql" ;;
  depths|depthsmoke)
    duckdb tpch.duckdb < "$CODE/depths.sql"
    grep -q 'PARITY_OK' results/parity_depths.txt || { cat results/parity_depths.txt; exit 1; } ;;
  qualify) python3 "$CODE/qualify.py" "$PWD/tpch.duckdb" ;;
  parity)
    duckdb < "$CODE/parity.sql"
    grep -q 'PARITY_OK' results/parity.txt || { cat results/parity.txt; exit 1; } ;;
  size-check)
    duckdb -csv tpch.duckdb -c "SELECT (SELECT count(*) FROM lineitem) lineitem,
      (SELECT count(*) FROM orders) orders, (SELECT count(*) FROM part) part,
      (SELECT count(*) FROM customer) customer, (SELECT count(*) FROM supplier) supplier,
      (SELECT count(*) FROM partsupp) partsupp, (SELECT count(*) FROM nation) nation,
      (SELECT count(*) FROM region) region;" > results/size-check.csv
    python3 "$CODE/check_size.py" gen.sql results/size-check.csv "$CODE/sizes.csv" ;;
  shapes)
    engine_flags vanilla parquet
    spark-submit "${JVM_FLAGS[@]}" "$CODE/shapes.py" --data "$PWD/data" --parents "${PARENTS:-128}" --fanout "${FANOUT:-64}" --width "${WIDTH:-8}" --seed "${SEED:-7}" ;;
  build-fmt)
    engine_flags vanilla "${2:?format required}"
    spark-submit "${JVM_FLAGS[@]}" "$CODE/write_fmt.py" --fmt "$2" --data "$PWD/data" ;;
  build-scale)
    "$CODE/run.sh" gen
    "$CODE/run.sh" size-check
    "$CODE/run.sh" conv
    "$CODE/run.sh" nested
    duckdb tpch.duckdb < "$CODE/invariants.sql"
    if [[ $(cat scale.txt) == 0.0083 ]]; then "$CODE/run.sh" qualify; fi
    "$CODE/run.sh" parity
    "$CODE/run.sh" depths
    "$CODE/run.sh" shapes
    for fmt in ${FORMATS:-parquet iceberg delta}; do
      [[ $fmt == parquet ]] || "$CODE/run.sh" build-fmt "$fmt"
    done
    python3 - "$CODE" "$PWD/data" <<'PY'
import sys
sys.path.insert(0,sys.argv[1])
from provenance import dataset
print('DATASET',dataset(sys.argv[2])['dataset_id'])
PY
    ;;
  spark) spark_run "${2:?engine required}" "${3:-parquet}" "${4:-$CODE/queries/manifest-full.json}" "${5:-3}" "${6:-no}" ;;
  lite) FORMATS=parquet matrix 1 no "${QUERIES:-$CODE/queries/manifest-full.json}" ;;
  full|comet-default|matrix) matrix "${RUNS:-3}" "${DROP_CACHES:-no}" "${QUERIES:-$CODE/queries/manifest-all.json}" ;;
  throughput) STREAMS=${STREAMS:-2} matrix "${RUNS:-3}" no "${QUERIES:-$CODE/queries/manifest-ds.json}" ;;
  bundle) python3 "$CODE/bundle.py" "${2:?campaign required}" ;;
  report) python3 "$CODE/report.py" "${2:?campaign directory required}" "${2}/report.md" ;;
  test) cd "$ROOT"; python3 -m unittest discover -s tests -v; shellcheck ndc/run.sh; bash -n ndc/run.sh ;;
  *) echo 'usage: run.sh setup|bootstrap <sf> <name>|build-scale|shapes|build-fmt <fmt>|size-check|parity|spark <engine> <fmt> [manifest] [runs]|lite|full|throughput|report <campaign>|test'; exit 2 ;;
esac
