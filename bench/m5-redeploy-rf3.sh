#!/bin/bash
# M5 RF=3 final benchmark prep: ISR catch-up fix redeploy + cluster restart.
set -e
M=/opt/m5/minimq
R=/mnt/e/workspace_work/mini-MQ/.m2repo/com/minimq

echo "== stop old mini-mq cluster =="
pkill -f 'com.minimq.broker.ClusterMain' 2>/dev/null || true
sleep 3
pgrep -f 'com.minimq.broker.ClusterMain' >/dev/null && { echo "ERROR: nodes still alive"; exit 1; }
echo "stopped"

echo "== wipe multi-era stale stores (fresh benchmark state) =="
for id in 0 1 2; do
  rm -rf /opt/m5/m5-cluster/node$id/commitlog
  rm -rf /opt/m5/m5-cluster/node$id/meta
  mkdir -p /opt/m5/m5-cluster/node$id/commitlog
done
# stale logs from prior eras confuse diagnosis; keep them but truncate
for id in 0 1 2; do
  : > /opt/m5/m5-cluster/node$id.log
  : > /opt/m5/m5-cluster/node$id.out
done
echo "stores wiped"

echo "== refresh module jars (ISR fix: replica + broker) =="
for mod in protocol storage replica broker client; do
  src=$R/mini-mq-$mod/0.1.0-SNAPSHOT/mini-mq-$mod-0.1.0-SNAPSHOT.jar
  cp "$src" $M/lib/
  echo "  updated $mod"
done

echo "== start 3-node cluster (OS_FLUSH, same as before) =="
start_node() {
  id=$1; port=$2; chaos=$3
  nohup java -Xms256m -Xmx1g \
    -cp "$M/classes:$M/lib/*" \
    com.minimq.broker.ClusterMain $id $port /opt/m5/m5-cluster/node$id/commitlog \
    OS_FLUSH 127.0.0.1:9701,127.0.0.1:9702,127.0.0.1:9703 $chaos 100000 \
    > /opt/m5/m5-cluster/node$id.out 2> /opt/m5/m5-cluster/node$id.log &
}
start_node 0 9701 19701
sleep 5
start_node 1 9702 19702
sleep 5
start_node 2 9703 19703

echo "== wait for leader election =="
for i in $(seq 1 30); do
  sleep 2
  if grep -l 'elected leader' /opt/m5/m5-cluster/node*.log >/dev/null 2>&1; then
    grep -h 'elected leader' /opt/m5/m5-cluster/node*.log | tail -1
    echo "CLUSTER-UP"
    break
  fi
  [ $i -eq 30 ] && { echo "ERROR: no leader elected in 60s"; tail -5 /opt/m5/m5-cluster/node*.log; exit 1; }
done

echo "== smoke: cluster produce 2k x 1 round, 1 concurrency =="
cd $M
java -cp "$M/classes:$M/lib/*" com.minimq.bench.M5Bench \
  --mode=produce --read-mode=tail --flush-mode=os \
  --cluster=127.0.0.1:9701,127.0.0.1:9702,127.0.0.1:9703 \
  --concurrency=1 --messages=2000 --warmup=200 --rounds=1 \
  --output=/opt/m5/results/minimq-cluster-smoke.jsonl
cat /opt/m5/results/minimq-cluster-smoke.jsonl
echo "== ISR sanity (evictions during smoke?) =="
grep -c 'evicted from ISR' /opt/m5/m5-cluster/node*.log || echo "0 evictions"
echo "SMOKE-DONE"
