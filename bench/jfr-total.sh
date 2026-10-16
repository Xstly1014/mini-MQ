#!/bin/bash
# Temp helper (M2-4b): sum sampled allocation bytes from a JFR recording.
# jfr print formats sizes as "24 bytes" / "14.0 kB" / "1.2 MB" — normalize.
set -e
f="$1"
echo "=== $f ==="
jfr print --events jdk.ObjectAllocationInNewTLAB "$f" 2>/dev/null \
  | grep -oE 'allocationSize = [0-9.]+ (bytes|kB|MB)' \
  | awk '{v=$3; u=$4; if(u=="kB") v*=1024; else if(u=="MB") v*=1048576; s+=v}
         END {printf "inTLAB sampled: %.1f MB\n", s/1048576}'
jfr print --events jdk.ObjectAllocationOutsideTLAB "$f" 2>/dev/null \
  | grep -oE 'allocationSize = [0-9.]+ (bytes|kB|MB)' \
  | awk '{v=$3; u=$4; if(u=="kB") v*=1024; else if(u=="MB") v*=1048576; s+=v}
         END {printf "outTLAB sampled: %.1f MB\n", s/1048576}'
