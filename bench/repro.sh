#!/bin/bash
# Temp helper (M2-3a debugging): run ZeroCopyRepro with full stderr visible.
set -e
cd /mnt/e/workspace_work/mini-MQ
mvn -q -Dmaven.repo.local=/mnt/c/repo -pl bench dependency:build-classpath \
    -Dmdep.outputFile=/tmp/cp.txt >/dev/null 2>&1 || true
CP="bench/target/classes:broker/target/classes:storage/target/classes:client/target/classes:protocol/target/classes:$(cat /tmp/cp.txt)"
exec java -cp "$CP" com.minimq.bench.ZeroCopyRepro "$@"
