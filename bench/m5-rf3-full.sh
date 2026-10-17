#!/bin/bash
# M5 final: full RF=3 produce benchmark, 6 concurrency tiers x 3 rounds.
M=/opt/m5/minimq
cd $M
java -cp "$M/classes:$M/lib/*" com.minimq.bench.M5Bench \
  --mode=produce --flush-mode=os \
  --cluster=127.0.0.1:9701,127.0.0.1:9702,127.0.0.1:9703 \
  --concurrency=1,2,4,8,16,32 --messages=60000 --warmup=6000 --rounds=3 \
  --output=/opt/m5/results/minimq-wsl-cluster-rf3-os.jsonl \
  > /opt/m5/results/rf3-bench.log 2>&1
echo "BENCH-EXIT=$?"
tail -3 /opt/m5/results/rf3-bench.log
wc -l /opt/m5/results/minimq-wsl-cluster-rf3-os.jsonl
