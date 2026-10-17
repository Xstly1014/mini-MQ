#!/bin/bash
# Temp helper (M2): run any bench main class with the full classpath.
# Usage: run.sh <MainClass> [args...]
set -e
cd /mnt/e/workspace_work/mini-MQ
if [ ! -f /tmp/cp.txt ]; then
    mvn -q -Dmaven.repo.local=/mnt/c/repo -pl bench dependency:build-classpath \
        -Dmdep.outputFile=/tmp/cp.txt >/dev/null 2>&1 || true
fi
CP="bench/target/classes:broker/target/classes:storage/target/classes:client/target/classes:protocol/target/classes:$(cat /tmp/cp.txt)"
exec java -cp "$CP" "$@"
