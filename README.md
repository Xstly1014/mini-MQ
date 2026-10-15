# mini-MQ

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Java](https://img.shields.io/badge/Java-8%2B-orange)
![Maven](https://img.shields.io/badge/Maven-3.5%2B-green)
![Status](https://img.shields.io/badge/status-M0%20storage%20kernel-brightgreen)

A lightweight distributed message queue written in Java: a RocketMQ-style single
CommitLog storage engine, combined with Kafka-style replication (fetch / leader
epoch / high watermark) and a Raft-style election kernel.

The design goal is **correct core mechanisms with measurable performance**, not
production completeness. Every milestone ships with unit tests and a
reproducible benchmark.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Client SDKs (producer / consumer)                  │  M1
├─────────────────────────────────────────────────────┤
│  Custom binary protocol (magic / version / CRC)     │  M1
├─────────────────────────────────────────────────────┤
│  Network layer — Netty                              │  M1
├─────────────────────────────────────────────────────┤
│  Storage — single CommitLog, mmap segments,         │
│  CRC-checked records, crash recovery                │  M0 (done)
├─────────────────────────────────────────────────────┤
│  Replication — fetch sync / epoch / HW +            │
│  Raft-style leader election                         │  M3
└─────────────────────────────────────────────────────┘
```

Key decisions (full rationale in [docs/design.md](docs/design.md)):

| Concern | Decision | Rationale |
|---|---|---|
| Log layout | Single CommitLog per broker (RocketMQ model) | All topics share one sequential write stream; per-partition files degrade to random I/O as partition count grows (Kafka model) |
| Segment I/O | Pre-allocated 1 GiB mmap segments | mmap writes are ~5x faster than per-record write syscalls (measured, see benchmark below) |
| Durability | Group commit (batch flush) | Per-record force is fsync-bound: ~4 orders of magnitude slower |
| Replication | Kafka-style data plane (fetch/epoch/HW), Raft only for election | A total-order Raft log is a throughput bottleneck for multi-partition workloads; epoch + HW achieve equivalent split-brain protection at lower cost |
| Coordination | No ZooKeeper; static broker config + lease heartbeats | Smaller failure domain, sufficient for the target scale |

## Storage kernel (M0, implemented)

- **Sequential CommitLog**: append-only, all records in one global offset space.
- **mmap segments**: 1 GiB pre-allocated files, named by their starting global
  offset; rolling happens automatically when a record does not fit.
- **Wire format** (little-endian): `4B total size | 1B magic | 4B CRC32 |
  8B timestamp | 8B key hash | 4B payload length | payload`.
- **Crash recovery**: on open, the tail segment is scanned; torn writes and
  CRC-corrupted records are truncated back to the last valid offset, garbage
  tail segments are dropped, valid empty tails are retained.
- **Test coverage**: 100k-record replay across segment rolls, torn-tail
  injection, on-disk CRC corruption, multi-segment continuity — 20/20 green.

## Write-path benchmark (bench-v0)

100,000 records x 1 KB payload, single thread, JDK 17, NVMe SSD
(reproduction instructions below; full report in [docs/bench-v0.md](docs/bench-v0.md)):

| Mode | Strategy | msg/s | MiB/s |
|------|----------|-------|-------|
| A | mmap, no force (page cache) | 3,158,569 | 3,099.6 |
| B | mmap, force per record | 83 | 0.1 |
| C | mmap, force every 100 records (group commit) | 4,748 | 4.7 |
| D | FileChannel + DirectBuffer, no force | 634,400 | 622.6 |

Findings: mmap puts outpace per-record write syscalls by ~5x; per-record
durability is ~38,000x slower (fsync-bound); group commit recovers ~57x of
that loss. Mode A measures page-cache write speed, not durable throughput —
the production design is mmap + group commit.

## Quick start

Requirements: JDK 8+ and Maven 3.5+.

Build and run the storage kernel tests:

```bash
mvn clean test
```

Run the write-path benchmark:

```bash
mvn -pl bench clean compile exec:java
# optional args: <count> <payloadSize> <forceEveryN>
mvn -pl bench clean compile exec:java -Dexec.args="100000 1000 100"
```

## Project layout

| Module | Purpose |
|--------|---------|
| `storage` | CommitLog storage kernel: record codec, mmap segment management, crash recovery |
| `bench` | Storage-level benchmarks |

## Roadmap

| Milestone | Scope | Status |
|-----------|-------|--------|
| M0 | Storage kernel: CommitLog, mmap segments, CRC recovery, write-path baseline | Done |
| M1 | Network layer: Netty, custom binary protocol, produce/consume loop, client SDK | Planned |
| M2 | Performance kernel: group commit, zero-copy consumption (sendfile) | Planned |
| M3 | Replication & election: fetch sync, leader epoch, high watermark, Raft-style voting | Planned |
| M4 | Fault-injection correctness: loss-free verification under failures | Planned |
| M5 | Full-system benchmark: throughput/P99 vs. Kafka on identical hardware, profiling | Planned |
| M6 | Documentation and release hardening | Planned |

## Scope and non-goals

Intentionally not implemented: transactional messages, cross-datacenter
replication, the full consumer rebalance protocol, and security
authentication. Each omission is a scoped trade-off of this project; see
[docs/design.md](docs/design.md) for the reasoning.

## Documentation

- [docs/design.md](docs/design.md) — storage engine design: Kafka / RocketMQ / Pulsar comparison, wire format, recovery semantics
- [docs/bench-v0.md](docs/bench-v0.md) — four-mode write-path benchmark report

## License

This project is licensed under the [MIT License](LICENSE).
