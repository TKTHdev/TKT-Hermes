# /// script
# requires-python = ">=3.11"
# dependencies = ["pandas", "matplotlib"]
# ///

"""
Usage: uv run --script scripts/plot_experiment_nodes.py <input.csv> <output.png>

Reads an experiment CSV with columns:
  Workload, Workers, Keys, Nodes, Throughput(ops/sec), Latency(ms)

Averages over repeated samples (same Workload/Workers/Keys/Nodes),
then plots Throughput vs Latency for each workload in separate subplots.
Lines are colored by node count. Key count is not used for color.
"""

import sys
import pandas as pd
import matplotlib.pyplot as plt

WORKLOAD_ORDER = ["ycsb-a", "ycsb-b", "ycsb-c"]
THROUGHPUT_COL = "Throughput(ops/sec)"
LATENCY_COL = "Latency(ms)"
NODES_COL = "Nodes"


def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input.csv> <output.png>", file=sys.stderr)
        sys.exit(1)

    csv_path = sys.argv[1]
    out_path = sys.argv[2]

    df = pd.read_csv(csv_path)
    required = {"Workload", "Workers", "Keys", NODES_COL, THROUGHPUT_COL, LATENCY_COL}
    missing = required - set(df.columns)
    if missing:
        print(f"Missing columns: {', '.join(sorted(missing))}", file=sys.stderr)
        sys.exit(1)

    # Average over repeated samples
    avg = (
        df.groupby(["Workload", "Workers", "Keys", NODES_COL])[[THROUGHPUT_COL, LATENCY_COL]]
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

    # Assign a consistent color per node count across all subplots
    all_nodes = sorted(avg[NODES_COL].unique())
    cmap = plt.colormaps["tab10"].resampled(max(len(all_nodes), 1))
    node_color = {n: cmap(i) for i, n in enumerate(all_nodes)}

    # Use a small marker cycle for key counts to reduce ambiguity
    marker_cycle = ["o", "s", "D", "^", "v", "X", "P"]

    for ax, workload in zip(axes, present_workloads):
        wdf = avg[avg["Workload"] == workload]

        for node in all_nodes:
            ndf = wdf[wdf[NODES_COL] == node]
            if ndf.empty:
                continue

            for i, key in enumerate(sorted(ndf["Keys"].unique())):
                kdf = ndf[ndf["Keys"] == key].sort_values("Workers")
                if kdf.empty:
                    continue

                xs = kdf[THROUGHPUT_COL].values
                ys = kdf[LATENCY_COL].values
                workers = kdf["Workers"].values

                ax.plot(
                    xs,
                    ys,
                    marker=marker_cycle[i % len(marker_cycle)],
                    color=node_color[node],
                    label=f"nodes={node}" if i == 0 else None,
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
                        color=node_color[node],
                    )

        ax.set_title(workload, fontsize=13, fontweight="bold")
        ax.set_xlabel("Throughput (ops/sec)")
        ax.set_ylabel("Latency (ms)")
        ax.legend(title="Node count", fontsize=8, title_fontsize=8)
        ax.grid(True, linestyle="--", alpha=0.4)

    fig.suptitle("Throughput vs Latency (averaged over samples)", fontsize=14)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Plot saved to {out_path}")


if __name__ == "__main__":
    main()
