# /// script
# requires-python = ">=3.11"
# dependencies = ["pandas", "matplotlib"]
# ///

"""
Usage: uv run --script scripts/plot_experiment.py <input.csv> <output.png>

Reads an experiment CSV with columns:
  Workload, Workers, Keys, Throughput(ops/sec), Latency(ms)

Averages over repeated samples (same Workload/Workers/Keys),
then plots Throughput vs Latency for each workload type in separate subplots.
Each point represents a worker count; lines are colored by key count.
"""

import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm

WORKLOAD_ORDER = ["ycsb-a", "ycsb-b", "ycsb-c"]
THROUGHPUT_COL = "Throughput(ops/sec)"
LATENCY_COL    = "Latency(ms)"


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input.csv> <output.png>", file=sys.stderr)
        sys.exit(1)

    csv_path = sys.argv[1]
    out_path  = sys.argv[2]

    df = pd.read_csv(csv_path)

    # Average over repeated samples
    avg = (
        df.groupby(["Workload", "Workers", "Keys"])[[THROUGHPUT_COL, LATENCY_COL]]
        .mean()
        .reset_index()
    )

    present_workloads = [w for w in WORKLOAD_ORDER if w in avg["Workload"].unique()]
    n = len(present_workloads)
    if n == 0:
        print("No data found in CSV.", file=sys.stderr)
        sys.exit(1)

    fig, axes = plt.subplots(1, n, figsize=(6 * n, 5), squeeze=False)
    axes = axes[0]

    # Assign a consistent color per key count across all subplots
    all_keys = sorted(avg["Keys"].unique())
    cmap = plt.colormaps["tab10"].resampled(len(all_keys))
    key_color = {k: cmap(i) for i, k in enumerate(all_keys)}

    for ax, workload in zip(axes, present_workloads):
        wdf = avg[avg["Workload"] == workload]

        for key in all_keys:
            kdf = wdf[wdf["Keys"] == key].sort_values("Workers")
            if kdf.empty:
                continue

            xs = kdf[THROUGHPUT_COL].values
            ys = kdf[LATENCY_COL].values
            workers = kdf["Workers"].values

            ax.plot(
                xs, ys,
                marker="o",
                color=key_color[key],
                label=f"keys={key}",
                linewidth=1.5,
                markersize=6,
            )

            for x, y, w in zip(xs, ys, workers):
                ax.annotate(
                    f"w={w}",
                    xy=(x, y),
                    xytext=(4, 4),
                    textcoords="offset points",
                    fontsize=7,
                    color=key_color[key],
                )

        ax.set_title(workload, fontsize=13, fontweight="bold")
        ax.set_xlabel("Throughput (ops/sec)")
        ax.set_ylabel("Latency (ms)")
        ax.legend(title="Key count", fontsize=8, title_fontsize=8)
        ax.grid(True, linestyle="--", alpha=0.4)

    fig.suptitle("Throughput vs Latency (averaged over samples)", fontsize=14)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Plot saved to {out_path}")


if __name__ == "__main__":
    main()
