#!/bin/bash
set -e
K=/opt/m5/kafka
AP=/opt/m5/ap/bin/asprof
mkdir -p /opt/m5/results
BS="localhost:9092,localhost:9094,localhost:9096"
run() {
  $K/bin/kafka-producer-perf-test.sh --topic m5rf3 --num-records 600000 \
    --record-size 1024 --throughput -1 \
    --producer-props bootstrap.servers=$BS acks=all linger.ms=5 \
    batch.size=16384 compression.type=none buffer.memory=67108864 \
    2>&1 | grep -vE '^\s*$'
}
echo "=== warmup (discarded) ==="
run > /opt/m5/results/kafka-rf3-warmup.txt
grep -E "records sent" /opt/m5/results/kafka-rf3-warmup.txt
for r in 1 2 3; do
  echo "=== rf3 round $r ==="
  if [ $r = 2 ]; then
    LEADER=$(ps aux | grep 'kafka.Kafka /tmp/kafka-n3.properties' | grep -v grep | awk '{print $2}')
    $AP -e cpu -d 20 -f /opt/m5/results/kafka-broker-leader-cpu.html $LEADER 2>&1 || true
  fi
  run | tee /opt/m5/results/kafka-rf3-round$r.txt | grep -E "records sent"
done
ls -la /opt/m5/results/kafka-broker-leader-cpu.html
