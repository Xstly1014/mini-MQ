#!/bin/bash
# mini-MQ 10-minute quickstart (M6): build -> 3-node cluster -> smoke with
# full produce/consume reconciliation -> teardown.
#
#   bash bench/quickstart.sh
#
# Expected verdict line: QUICKSTART-PASS sent=10000 acked=10000 lost=0
# (M5Bench reconciles: consumed == messages means every produced record was
# acked and read back exactly once; any loss/duplicate breaks the count.)
#
# Requirements: JDK 17 (build: bench module compiles at release 17; the
# core modules target Java 8), Maven 3.5+, bash (Linux / WSL2 tested).
# Ports 9701-9703 must be free.

set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$(pwd)
RUN="$ROOT/.quickstart"
MVNREPO="$ROOT/.m2repo"
PEERS="127.0.0.1:9701,127.0.0.1:9702,127.0.0.1:9703"
MESSAGES=10000
pids=()

cleanup() {
  if [ ${#pids[@]} -gt 0 ]; then
    kill "${pids[@]}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "== [1/4] build (tests skipped) =="
# No `clean`: on WSL2/drvfs the protobuf plugin fails to recreate
# protocol/target/protoc-dependencies after clean wipes it (proto path
# element is not a directory). Fresh/incremental builds work everywhere.
mvn -q install -Dmaven.test.skip=true -Dmaven.repo.local="$MVNREPO"

echo "== [2/4] assemble runtime (module jars + deps + bench classes) =="
rm -rf "$RUN"
mkdir -p "$RUN/lib" "$RUN/classes" "$RUN/store"
cp -r bench/target/classes/. "$RUN/classes/"
mvn -q -pl bench dependency:copy-dependencies \
  -Dmaven.repo.local="$MVNREPO" \
  -DoutputDirectory="$RUN/lib" -DincludeScope=runtime
CP="$RUN/classes:$RUN/lib/*"

echo "== [3/4] start 3-node cluster (OS_FLUSH, dedup on) =="
for p in 9701 9702 9703; do
  if ss -tln 2>/dev/null | grep -q ":$p " || (exec 3<>/dev/tcp/127.0.0.1/$p) 2>/dev/null; then
    echo "ERROR: port $p already in use (a previous cluster still running?)"
    echo "       free it, e.g.: pkill -f com.minimq.broker.ClusterMain"
    exit 1
  fi
done
start_node() {
  id=$1; port=$2; chaos=$3
  nohup java -Xms256m -Xmx1g -cp "$CP" \
    com.minimq.broker.ClusterMain "$id" "$port" "$RUN/store/node$id" \
    OS_FLUSH "$PEERS" "$chaos" 100000 \
    > "$RUN/node$id.out" 2> "$RUN/node$id.log" &
  pids+=($!)
}
start_node 0 9701 19701
sleep 2
start_node 1 9702 19702
sleep 2
start_node 2 9703 19703

for i in $(seq 1 30); do
  sleep 2
  if grep -l 'elected leader' "$RUN"/node*.log >/dev/null 2>&1; then
    grep -h 'elected leader' "$RUN"/node*.log | tail -1
    echo "CLUSTER-UP"
    break
  fi
  [ $i -eq 30 ] && { echo "ERROR: no leader elected in 60s"; tail -n 20 "$RUN"/node*.log; exit 1; }
done

echo "== [4/4] smoke: ${MESSAGES} msgs @ c=4, 1KB, full reconcile =="
java -cp "$CP" com.minimq.bench.M5Bench \
  --mode=produce --read-mode=tail --flush-mode=os \
  --cluster="$PEERS" \
  --concurrency=4 --messages=$MESSAGES --warmup=1000 --rounds=1 \
  --output="$RUN/smoke.jsonl"
cat "$RUN/smoke.jsonl"

consumed=$(grep -o '"consumed":[0-9]*' "$RUN/smoke.jsonl" | head -1 | cut -d: -f2)
p99=$(grep -o '"p99Ms":[0-9.]*' "$RUN/smoke.jsonl" | head -1 | cut -d: -f2)
evictions=$({ grep -h 'evicted from ISR' "$RUN"/node*.log || true; } | wc -l)

echo "ISR evictions during smoke: $evictions"
if [ "$consumed" -eq "$MESSAGES" ] && [ "$evictions" -eq "0" ]; then
  echo "QUICKSTART-PASS sent=$MESSAGES acked=$MESSAGES lost=0 dup=0 P99=${p99}ms"
else
  echo "QUICKSTART-FAIL consumed=$consumed/$MESSAGES evictions=$evictions"
  exit 1
fi
