#!/bin/bash
# Temp helper (M2-4b): 100k reconcile regression on the full M2 stack.
set -e
cd /mnt/e/workspace_work/mini-MQ
CP="bench/target/classes:broker/target/classes:storage/target/classes:client/target/classes:protocol/target/classes:$(cat /tmp/cp.txt)"
java -cp "$CP" com.minimq.bench.Reconcile
