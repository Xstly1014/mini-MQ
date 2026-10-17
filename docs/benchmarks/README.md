# Benchmarks

Evidence index — where every number lives and how to regenerate it.

| What | Where |
|---|---|
| README benchmark tables (generated) | `README-bench.md` (this folder, embedded into the top-level README) |
| Generator script (JSONL -> tables, no hand-copying) | `bench/gen-readme-bench.ps1` |
| Raw sweep JSONL, Kafka txt, flame graphs, run logs | `result/m5-20261017-verified/` (single broker), `result/m5-20261017-rf3-wsl/` (3-node RF=3 + Kafka), `result/m5-chaos-matrix.txt` (chaos matrix) |
| M2 write-path / group-commit scan archives | `docs/bench-v0.md`, `docs/bench-v1.md`, `docs/bench-v2.md` |
| Full tuning reports & four-bucket Kafka attribution | `docs/tuning.md` |
| One-command reruns | `bench/m5-run.ps1` (single broker), `bench/m5-redeploy-rf3.sh` + `bench/m5-rf3-full.sh` (cluster), `bench/m5-kafka-rf3.sh` (Kafka), `bench/quickstart.sh` (10-min smoke) |

Regenerate the README tables after a new verified run:

```bash
powershell -NoProfile -ExecutionPolicy Bypass -File bench/gen-readme-bench.ps1 -OutFile docs/benchmarks/README-bench.md
```

The medians are recomputed from the raw `samples` field (3 rounds per row),
not trusted from pre-computed columns; Kafka numbers come from the raw
`kafka-rf3-round*.txt` lines.
