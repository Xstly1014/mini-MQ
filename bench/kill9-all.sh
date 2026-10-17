#!/bin/bash
# Temp helper (M2-4b): run the kill -9 reconcile across all three
# durability tiers and archive the log (DoD k03/k05 evidence).
set -e
cd /mnt/e/workspace_work/mini-MQ
CP="bench/target/classes:broker/target/classes:storage/target/classes:client/target/classes:protocol/target/classes:$(cat /tmp/cp.txt)"
java -cp "$CP" com.minimq.bench.Kill9Reconcile SYNC_FLUSH 200000 1024
echo "=== ASYNC ==="
java -cp "$CP" com.minimq.bench.Kill9Reconcile ASYNC_FLUSH 200000 1024
echo "=== OS ==="
java -cp "$CP" com.minimq.bench.Kill9Reconcile OS_FLUSH 200000 1024
