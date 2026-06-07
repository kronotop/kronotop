"""
Usage:
  uv run --with pandas --with matplotlib scripts/histogram.py <csv_file> [<csv_file> ...]
  uv run --with pandas --with matplotlib scripts/histogram.py latencies/
  uv run --with pandas --with matplotlib scripts/histogram.py latencies/ --save report.png
"""

import sys
import pathlib
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np


def parse_metadata(path: pathlib.Path) -> dict:
    meta = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            if not line.startswith("#"):
                break
            line = line[1:].strip()
            if ":" in line:
                key, _, val = line.partition(":")
                meta[key.strip()] = val.strip()
    return meta


def plot_file(path: pathlib.Path, ax: plt.Axes) -> None:
    meta = parse_metadata(path)
    df = pd.read_csv(path, comment="#")
    ms = df["latency_ns"] / 1_000_000

    p50 = np.percentile(ms, 50)
    p95 = np.percentile(ms, 95)
    p99 = np.percentile(ms, 99)
    p999 = np.percentile(ms, 99.9)
    clip = np.percentile(ms, 99.5)

    data = ms[ms <= clip]
    ax.hist(data, bins=80, color="#4C72B0", edgecolor="none", alpha=0.85)

    for val, label, color in [
        (p50, f"P50 {p50:.2f} ms", "#2ca02c"),
        (p95, f"P95 {p95:.2f} ms", "#ff7f0e"),
        (p99, f"P99 {p99:.2f} ms", "#d62728"),
        (p999, f"P99.9 {p999:.2f} ms", "#8c564b"),
    ]:
        ax.axvline(val, color=color, linewidth=1.8, linestyle="--", label=label)

    scenario = meta.get("scenario", path.stem.rsplit("_", 2)[0].upper().replace("_", " "))
    ax.set_title(scenario, fontsize=14, fontweight="bold", pad=10)
    ax.set_xlabel("Latency (ms)", fontsize=11)
    ax.set_ylabel("Count", fontsize=11)
    ax.xaxis.set_major_formatter(ticker.FormatStrFormatter("%.2f"))
    ax.legend(fontsize=10, framealpha=0.9)
    ax.grid(axis="y", linewidth=0.4, alpha=0.6)
    ax.spines[["top", "right"]].set_visible(False)

    # build info line from metadata + computed stats
    n = len(ms)
    tail_pct = (ms > clip).sum() / n * 100
    parts = []
    if "threads" in meta:
        parts.append(f"threads={meta['threads']}")
    if "queries" in meta:
        parts.append(f"n={int(meta['queries']):,}")
    if "qps" in meta:
        parts.append(f"QPS={float(meta['qps']):,.0f}")
    if "wall_clock_ms" in meta:
        parts.append(f"wall={float(meta['wall_clock_ms'])/1000:.2f}s")
    parts.append(f"tail(>{clip:.1f}ms)={tail_pct:.1f}%")

    ax.text(
        0.5, 0.01,
        "  |  ".join(parts),
        transform=ax.transAxes, ha="center", va="bottom",
        fontsize=8.5, color="#444444",
        bbox=dict(facecolor="white", edgecolor="none", alpha=0.75, pad=3),
    )


def collect_paths(args: list[str]) -> list[pathlib.Path]:
    paths = []
    for arg in args:
        p = pathlib.Path(arg)
        if p.is_dir():
            paths.extend(sorted(p.glob("*.csv")))
        elif p.suffix == ".csv":
            paths.append(p)
    return paths


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(1)

    save_path = None
    if "--save" in args:
        idx = args.index("--save")
        save_path = args[idx + 1]
        args = args[:idx] + args[idx + 2:]

    paths = collect_paths(args)
    if not paths:
        print("No CSV files found.")
        sys.exit(1)

    n = len(paths)
    cols = min(n, 2)
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(8 * cols, 5.5 * rows))
    axes = np.array(axes).reshape(-1) if n > 1 else [axes]

    for ax, path in zip(axes, paths):
        plot_file(path, ax)

    for ax in axes[n:]:
        ax.set_visible(False)

    fig.tight_layout(pad=3.0)

    if save_path:
        fig.savefig(save_path, dpi=200, bbox_inches="tight")
        print(f"Saved → {pathlib.Path(save_path).resolve()}")
        plt.close(fig)
    else:
        plt.show()


if __name__ == "__main__":
    main()
