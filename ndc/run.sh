#!/usr/bin/env bash
# One pinned checkout; datasets and immutable campaigns live in an explicit workspace.
set -euo pipefail
CODE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(dirname "$CODE")
usage() { printf '%s\n' 'usage: run.sh setup|bootstrap <sf> <name>|build-scale|shapes|build-fmt <fmt>|size-check|invariants|qualify-references|qualify-engine <engine> [fmt]|spark <engine> <fmt> [manifest] [runs]|matrix|latency|shared-throughput|maintenance|plan <out.json>|report <campaign>|compare <label=result.json>...|experiment <spec.json> --out <directory>|bundle <campaign>|check (SUITE selects workloads; test/full/throughput remain aliases)'; }
if [[ ${1:-} == --help || ${1:-} == -h || ${1:-} == help ]]; then usage; exit 0; fi
if [[ $# == 0 ]]; then usage; exit 2; fi
if [[ ${NDC_IN_ENV:-} != 1 ]]; then
  exec nix develop "path:$ROOT/nix" -c env NDC_IN_ENV=1 bash "$0" "$@"
fi
if [[ $1 == check || $1 == test ]]; then
  cd "$ROOT"
  python3 -m unittest discover -s tests -v
  shellcheck ndc/run.sh
  bash -n ndc/run.sh
  exit 0
fi
if [[ $1 == compare || $1 == experiment ]]; then
  command=$1; shift
  exec python3 "$CODE/$command.py" "$@"
fi
if [[ ${NDC_RESOURCE_SCOPE:-0} != 1 && ( -n ${NDC_CPU_LIMIT:-} || -n ${NDC_MEMORY_LIMIT:-} ) ]]; then
  scope=(--user --scope --quiet)
  if [[ -n ${NDC_CPU_LIMIT:-} ]]; then
    [[ $NDC_CPU_LIMIT =~ ^[1-9][0-9]{0,3}$ ]] || { echo 'NDC_CPU_LIMIT must be a positive integer below 10000' >&2; exit 2; }
    scope+=(-p "CPUQuota=$((NDC_CPU_LIMIT * 100))%")
  fi
  if [[ -n ${NDC_MEMORY_LIMIT:-} ]]; then
    [[ $NDC_MEMORY_LIMIT =~ ^[1-9][0-9]*[KMGTP]?$ ]] || { echo 'Invalid NDC_MEMORY_LIMIT (example: 16G)' >&2; exit 2; }
    scope+=(-p "MemoryMax=$NDC_MEMORY_LIMIT")
  fi
  exec systemd-run "${scope[@]}" env NDC_RESOURCE_SCOPE=1 bash "$0" "$@"
fi
export NDC_WORKSPACE=${NDC_WORKSPACE:-$ROOT/workspaces/default}
mkdir -p "$NDC_WORKSPACE"
cd "$NDC_WORKSPACE"
export NDC_WORKSPACE="$PWD"
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
printf 'NDC stage=%s starting\n' "$1"
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
  engine_flags "$eng" "$fmt" || return $?
  if [[ ! -e $campaign/plan.json ]]; then
    freeze_plan "$campaign/plan.json" "$q" "$runs" "${NDC_PHASE:-matrix}" || return $?
  fi
  local args=(--fmt "$fmt" --data "$PWD/data" --out "$out" --runs "$runs"
    --sf "$(cat scale.txt)" --campaign-id "$(basename "$campaign")" --streams "${STREAMS:-1}"
    --warmups "${WARMUPS:-1}" --seed "${QUERY_SEED:-${SEED:-7}}"
    --plan "$campaign/plan.json" --phase "${NDC_PHASE:-matrix}" --cache "${CACHE:-uncontrolled}"
    --layout-mode "${LAYOUT_MODE:-matched}" --validation "${VALIDATION:-collect}")
  [[ $eng != comet ]] || args+=(--comet)
  [[ $drop != yes ]] || args+=(--drop-caches)
  printf 'NDC candidate=%s engine=%s format=%s starting\n' "${NDC_CANDIDATE:-$eng}" "$eng" "$fmt"
  : > "$campaign/spark_${eng}_${fmt}.stdout"
  spark-submit "${JVM_FLAGS[@]}" "$CODE/spark_poc.py" "${args[@]}" > "$campaign/spark_${eng}_${fmt}.stdout" 2>&1 &
  local spid=$! rc=0
  tail --pid="$spid" --sleep-interval=0.1 -n +1 -f "$campaign/spark_${eng}_${fmt}.stdout" | grep --line-buffered '^NDC ' &
  local progress_pid=$!
  python3 "$CODE/monitor.py" "$spid" "$mon" "${MONITOR_INTERVAL:-0.2}" &
  local mpid=$!
  wait "$spid" || rc=$?
  wait "$mpid" || rc=1
  wait "$progress_pid" || true
  if [[ -f $out ]]; then
    python3 "$CODE/merge_monitor.py" "$out" "$mon" || rc=1
  fi
  [[ $rc == 0 ]] || tail -20 "$campaign/spark_${eng}_${fmt}.stdout"
  printf 'NDC candidate=%s engine=%s format=%s exit=%s\n' "${NDC_CANDIDATE:-$eng}" "$eng" "$fmt" "$rc"
  return "$rc"
}

freeze_plan() {
  local out=$1 queries=$2 runs=$3 phase=$4
  local selection=(--suite "${SUITE:-all}")
  [[ -z $queries ]] || selection=(--queries "$queries")
  python3 "$CODE/schedule.py" "${selection[@]}" --out "$out" --phase "$phase" \
    --runs "$runs" --streams "${STREAMS:-1}" --warmups "${WARMUPS:-1}" --seed "${QUERY_SEED:-${SEED:-7}}"
}

matrix() { # Continue for diagnostics, but any failed cell fails the campaign.
  local runs=${1:-3} drop=${2:-no} q=${3:-} failed=0
  export NDC_PHASE=${4:-${NDC_PHASE:-matrix}}
  local formats=${FORMATS:-parquet iceberg delta} engines=${ENGINES:-vanilla comet}
  local base=${NDC_WORKSPACE:-$PWD}
  export NDC_CAMPAIGN_DIR=${NDC_CAMPAIGN_DIR:-$base/results/$(date -u +%Y%m%dT%H%M%S)-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')}
  [[ ! -e $NDC_CAMPAIGN_DIR ]] || { echo "Campaign already exists: $NDC_CAMPAIGN_DIR" >&2; return 2; }
  mkdir -p "$NDC_CAMPAIGN_DIR"
  freeze_plan "$NDC_CAMPAIGN_DIR/plan.json" "$q" "$runs" "$NDC_PHASE" || return $?
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
  if [[ $failed != 0 ]]; then
    python3 - "$NDC_CAMPAIGN_DIR/campaign.json" <<'PYCODE'
import json,sys
from pathlib import Path
path=Path(sys.argv[1]);record=json.loads(path.read_text());record['status']='failed'
path.write_text(json.dumps(record,indent=2))
PYCODE
  fi
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
  qualify|qualify-references) python3 "$CODE/qualify.py" "$PWD/tpch.duckdb" ;;
  qualify-engine) python3 "$CODE/qualification.py" "${2:?engine required}" "${3:-parquet}" ;;
  invariants) duckdb tpch.duckdb < "$CODE/invariants.sql" ;;
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
    spark-submit "${JVM_FLAGS[@]}" "$CODE/shapes.py" --data "$PWD/data" --parents "${PARENTS:-128}" --fanout "${FANOUT:-64}" --width "${WIDTH:-8}" --seed "${DATA_SEED:-${SEED:-7}}" ;;
  build-fmt)
    engine_flags vanilla "${2:?format required}"
    spark-submit "${JVM_FLAGS[@]}" "$CODE/write_fmt.py" --fmt "$2" --data "$PWD/data" ;;
  build-scale)
    "$CODE/run.sh" gen
    "$CODE/run.sh" size-check
    "$CODE/run.sh" conv
    "$CODE/run.sh" nested
    "$CODE/run.sh" invariants
    if [[ $(cat scale.txt) == 0.0083 ]]; then "$CODE/run.sh" qualify-references; fi
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
  lite) FORMATS=parquet SUITE=${SUITE:-tpch} matrix 1 no "${QUERIES:-}" ;;
  full|comet-default|matrix) matrix "${RUNS:-3}" "${DROP_CACHES:-no}" "${QUERIES:-}" ;;
  latency) SUITE=${SUITE:-read} STREAMS=1 matrix "${RUNS:-3}" "${DROP_CACHES:-no}" "${QUERIES:-}" latency ;;
  maintenance) SUITE=${SUITE:-maintenance} STREAMS=1 matrix "${RUNS:-3}" "${DROP_CACHES:-no}" "${QUERIES:-}" maintenance ;;
  plan) freeze_plan "${2:?output JSON path required}" "${QUERIES:-}" "${RUNS:-3}" "${NDC_PHASE:-matrix}" ;;
  throughput|shared-throughput) SUITE=${SUITE:-ds} STREAMS=${STREAMS:-2} matrix "${RUNS:-3}" no "${QUERIES:-}" shared-throughput ;;
  bundle) python3 "$CODE/bundle.py" "${2:?campaign required}" ;;
  report) python3 "$CODE/report.py" "${2:?campaign directory required}" "${2}/report.md" ;;
  *) usage; exit 2 ;;
esac
