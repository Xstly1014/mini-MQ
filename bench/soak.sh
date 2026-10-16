#!/bin/bash
# Temp helper (M2-4b): launch the 30-min soak with Netty paranoid leak
# detection enabled (DoD k09: zero LEAK under paranoid).
# Usage: soak.sh [minutes=30] [producers=8] [payload=512] [rate=10000]
set -e
cd /mnt/e/workspace_work/mini-MQ
if [ ! -f /tmp/cp.txt ]; then
    mvn -q -Dmaven.repo.local=/mnt/c/repo -pl bench dependency:build-classpath \
        -Dmdep.outputFile=/tmp/cp.txt >/dev/null 2>&1 || true
fi
CP="bench/target/classes:broker/target/classes:storage/target/classes:client/target/classes:protocol/target/classes:$(cat /tmp/cp.txt)"
MINUTES=${1:-30}
PRODUCERS=${2:-8}
PAYLOAD=${3:-512}
RATE=${4:-10000}
exec java -Dio.netty.leakDetection.level=paranoid -cp "$CP" \
    com.minimq.bench.SoakV2 "$MINUTES" "$PRODUCERS" "$PAYLOAD" "$RATE"
