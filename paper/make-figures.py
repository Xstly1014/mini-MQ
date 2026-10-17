#!/usr/bin/env python3
"""Generate all evaluation figures for the mini-MQ paper from raw result data.

Sources (all committed to the repository):
  - result/m5-20261017-verified/produce.jsonl
        single-broker SYNC_FLUSH sweep, Windows, 6 concurrency tiers x 3 rounds
  - result/m5-20261017-rf3-wsl/minimq-wsl-produce-os.jsonl
        single-broker OS_FLUSH sweep, WSL2, 2 tiers x 3 rounds
  - result/m5-20261017-rf3-wsl/minimq-wsl-cluster-rf3-os.jsonl
        3-node RF=3 end-to-end sweep, WSL2, 6 tiers x 3 rounds
  - result/m5-20261017-rf3-wsl/kafka-rf3-round{1,2,3}.txt
        same-host Kafka 4.1.2 (KRaft) reference, 3 rounds
  - Group-commit matrices (M2 BenchV2): tables recorded in docs/bench-v2.md;
    values below are transcribed from those recorded tables.

Medians are recomputed here from the per-round `samples` field of each JSONL
record; no figure number is hand-copied from any summary document.

Output: paper/figures/*.pdf (vector) for inclusion with \includegraphics.
"""

import json
import re
import statistics
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "paper" / "figures"
OUT.mkdir(parents=True, exist_ok=True)

C_SYNC = "#2563eb"   # blue
C_OS = "#059669"     # green
C_KAFKA = "#d97706"  # amber
C_P99 = "#dc2626"    # red

plt.rcParams.update({
    "font.size": 9,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.30,
    "grid.linewidth": 0.5,
    "legend.frameon": False,
})


def jsonl(path):
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            yield json.loads(line)


def med_with_rounds(rec):
    """Return (median throughput, median p99, [(tput, p99) per round])."""
    rounds = [tuple(float(x) for x in s.split(","))
              for s in rec["samples"].split(";")]
    tputs = [r[0] for r in rounds]
    p99s = [r[3] for r in rounds]
    return statistics.median(tputs), statistics.median(p99s), rounds


# ---------------------------------------------------------------------------
# Figure: group commit (docs/bench-v2.md, M2 BenchV2 runs)
# ---------------------------------------------------------------------------
def fig_groupcommit():
    producers = [1, 2, 4, 8, 16]
    os_flush = [526315, 689655, 1000000, 800000, 400000]
    sync_flush = [212765, 333333, 540540, 714285, 434782]
    msgs_per_force = [241.0, 454.5, 740.7, 909.1, 645.2]

    # 5x4 scan: linger(us) x count -> throughput msg/s
    lingers = [0, 200, 500, 1000, 2000]
    counts = [16, 64, 256, 1024]
    scan = np.array([
        [18691, 166666, 190476, 186915],
        [198019, 178571, 208333, 212765],
        [208333, 210526, 204081, 212765],
        [210526, 210526, 202020, 212765],
        [112359, 111731, 112359, 110497],
    ])

    fig, (ax1, ax2) = plt.subplots(
        1, 2, figsize=(7.0, 2.9), constrained_layout=True)

    # (a) throughput vs producers, log scale
    ax1.plot(producers, os_flush, "s-", color=C_OS, label="OS_FLUSH (ack before force)")
    ax1.plot(producers, sync_flush, "o-", color=C_SYNC, label="SYNC_FLUSH (ack after force)")
    for x, y, m in zip(producers, sync_flush, msgs_per_force):
        ax1.annotate(f"{m:.0f}", (x, y), textcoords="offset points",
                     xytext=(0, -14), ha="center", fontsize=7.5, color=C_SYNC)
    ax1.axhline(83, color=C_P99, linestyle="--", linewidth=1)
    ax1.annotate("per-record force: 83 msg/s", (1, 83), textcoords="offset points",
                 xytext=(6, 5), fontsize=7.5, color=C_P99)
    ax1.set_xscale("log", base=2)
    ax1.set_yscale("log")
    ax1.set_xticks(producers)
    ax1.set_xticklabels([str(p) for p in producers])
    ax1.set_xlabel("Producers")
    ax1.set_ylabel("Throughput (msg/s, log scale)")
    ax1.set_title("(a) Group-commit throughput vs. concurrency\n(blue: records per fsync)", fontsize=9)
    ax1.legend(loc="lower left", fontsize=8)
    ax1.set_ylim(40, 3e6)

    # (b) linger x count heatmap
    im = ax2.imshow(scan, cmap="YlGnBu", aspect="auto")
    ax2.set_xticks(range(len(counts)), [str(c) for c in counts])
    ax2.set_yticks(range(len(lingers)), [str(l) for l in lingers])
    ax2.set_xlabel("flushBatchCount")
    ax2.set_ylabel("flushLingerUs")
    ax2.set_title("(b) SYNC throughput scan: linger x count (msg/s)", fontsize=9)
    ax2.grid(False)
    thresh = scan.max() * 0.55
    for i in range(scan.shape[0]):
        for j in range(scan.shape[1]):
            v = scan[i, j]
            ax2.text(j, i, f"{v:,}", ha="center", va="center", fontsize=7.5,
                     color="white" if v > thresh else "#111827")
    cb = fig.colorbar(im, ax=ax2, shrink=0.85, pad=0.02)
    cb.ax.tick_params(labelsize=7)

    fig.savefig(OUT / "fig-groupcommit.pdf")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Figure: single-broker produce sweep (verified JSONL)
# ---------------------------------------------------------------------------
def fig_single():
    recs = sorted(jsonl(ROOT / "result/m5-20261017-verified/produce.jsonl"),
                  key=lambda r: r["concurrency"])
    xs, med_t, med_p, r_t, r_p = [], [], [], [], []
    for rec in recs:
        t, p, rounds = med_with_rounds(rec)
        xs.append(rec["concurrency"])
        med_t.append(t)
        med_p.append(p)
        r_t.append([x[0] for x in rounds])
        r_p.append([x[1] for x in rounds])

    os_recs = sorted(jsonl(ROOT / "result/m5-20261017-rf3-wsl/minimq-wsl-produce-os.jsonl"),
                     key=lambda r: r["concurrency"])
    os_x = [r["concurrency"] for r in os_recs]
    os_t = [med_with_rounds(r)[0] for r in os_recs]
    os_p = [med_with_rounds(r)[1] for r in os_recs]

    fig, (ax1, ax2) = plt.subplots(
        1, 2, figsize=(7.0, 2.55), constrained_layout=True)

    ax1.plot(xs, med_t, "o-", color=C_SYNC, label="SYNC_FLUSH (Windows)")
    for x, rt in zip(xs, r_t):
        ax1.scatter([x] * len(rt), rt, s=8, color=C_SYNC, alpha=0.45, zorder=2)
    ax1.plot(os_x, os_t, "s--", color=C_OS, label="OS_FLUSH (WSL2)")
    ax1.set_xscale("log", base=2)
    ax1.set_xticks(xs)
    ax1.set_xticklabels([str(x) for x in xs])
    ax1.set_xlabel("Producer concurrency")
    ax1.set_ylabel("Throughput (msg/s)")
    ax1.set_title("(a) Single broker: 1 KiB produce throughput", fontsize=9)
    ax1.legend(fontsize=8)

    ax2.plot(xs, med_p, "o-", color=C_SYNC, label="SYNC_FLUSH P99 (Windows)")
    for x, rp in zip(xs, r_p):
        ax2.scatter([x] * len(rp), rp, s=8, color=C_SYNC, alpha=0.45, zorder=2)
    ax2.set_xscale("log", base=2)
    ax2.set_xticks(xs)
    ax2.set_xticklabels([str(x) for x in xs])
    ax2.set_xlabel("Producer concurrency")
    ax2.set_ylabel("Produce-ack P99 (ms)")
    ax2.set_title("(b) Tail latency stays flat on the throughput plateau", fontsize=9)
    ax2.legend(fontsize=8)

    fig.savefig(OUT / "fig-single.pdf")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Figure: 3-node RF=3 end-to-end vs same-host Kafka
# ---------------------------------------------------------------------------
def kafka_rounds():
    tputs, p99s = [], []
    pat = re.compile(
        r"(\d[\d.]*) records/sec.*?(\d+) ms 99th")
    for i in (1, 2, 3):
        txt = (ROOT / f"result/m5-20261017-rf3-wsl/kafka-rf3-round{i}.txt").read_text()
        for line in txt.splitlines():
            if line.startswith("600000 records sent"):
                m = pat.search(line)
                if m:
                    tputs.append(float(m.group(1)))
                    p99s.append(float(m.group(2)))
    return tputs, p99s


def fig_cluster():
    recs = sorted(jsonl(ROOT / "result/m5-20261017-rf3-wsl/minimq-wsl-cluster-rf3-os.jsonl"),
                  key=lambda r: r["concurrency"])
    xs, med_t, med_p, r_t, r_p = [], [], [], [], []
    for rec in recs:
        t, p, rounds = med_with_rounds(rec)
        xs.append(rec["concurrency"])
        med_t.append(t)
        med_p.append(p)
        r_t.append([x[0] for x in rounds])
        r_p.append([x[1] for x in rounds])

    kt, kp = kafka_rounds()
    k_t, k_p = statistics.median(kt), statistics.median(kp)
    ratio_t = med_t[2] / k_t          # c=4
    ratio_p = med_p[2] / k_p

    fig, (ax1, ax2) = plt.subplots(
        1, 2, figsize=(7.0, 2.55), constrained_layout=True)

    ax1.plot(xs, med_t, "o-", color=C_SYNC, label="mini-MQ (RF=3, acks=all)")
    for x, rt in zip(xs, r_t):
        ax1.scatter([x] * len(rt), rt, s=8, color=C_SYNC, alpha=0.45, zorder=2)
    ax1.axhline(k_t, color=C_KAFKA, linestyle="--", linewidth=1.2)
    ax1.annotate(f"Kafka 4.1.2 median: {k_t:,.0f} msg/s",
                 (32, k_t), textcoords="offset points", xytext=(-4, 6),
                 ha="right", fontsize=8, color=C_KAFKA)
    ax1.annotate(f"mini-MQ / Kafka = {ratio_t:.0%} (c=4)",
                 xy=(4, med_t[2]), xytext=(5.5, med_t[2] * 0.86),
                 arrowprops=dict(arrowstyle="->", lw=0.8), fontsize=8)
    ax1.set_xscale("log", base=2)
    ax1.set_xticks(xs)
    ax1.set_xticklabels([str(x) for x in xs])
    ax1.set_xlabel("Producer concurrency")
    ax1.set_ylabel("End-to-end throughput (msg/s)")
    ax1.set_ylim(0, max(med_t) * 1.22)
    ax1.set_title("(a) 3-node cluster, WSL2 loopback", fontsize=9)
    ax1.legend(fontsize=8, loc="lower left")

    ax2.plot(xs, med_p, "o-", color=C_SYNC, label="mini-MQ P99")
    for x, rp in zip(xs, r_p):
        ax2.scatter([x] * len(rp), rp, s=8, color=C_SYNC, alpha=0.45, zorder=2)
    ax2.axhline(k_p, color=C_KAFKA, linestyle="--", linewidth=1.2)
    ax2.annotate(f"Kafka P99: {k_p:.0f} ms",
                 (32, k_p), textcoords="offset points", xytext=(-4, 6),
                 ha="right", fontsize=8, color=C_KAFKA)
    ax2.annotate(f"{ratio_p:.2f}x Kafka (c=4)",
                 xy=(4, med_p[2]), xytext=(6, med_p[2] + 240),
                 arrowprops=dict(arrowstyle="->", lw=0.8), fontsize=8)
    ax2.set_xscale("log", base=2)
    ax2.set_xticks(xs)
    ax2.set_xticklabels([str(x) for x in xs])
    ax2.set_xlabel("Producer concurrency")
    ax2.set_ylabel("End-to-end P99 (ms)")
    ax2.set_ylim(0, 2100)
    ax2.set_title("(b) P99 latency vs. same-host Kafka", fontsize=9)
    ax2.legend(fontsize=8, loc="lower left")

    fig.savefig(OUT / "fig-cluster.pdf")
    plt.close(fig)
    print(f"kafka medians: tput={k_t} p99={k_p}; "
          f"mini-mq c=4 tput={med_t[2]} p99={med_p[2]}")


if __name__ == "__main__":
    fig_groupcommit()
    fig_single()
    fig_cluster()
    print("figures written to", OUT)
