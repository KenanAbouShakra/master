"""Generate the two observed holdout figures for Chapter 5."""
import argparse
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
DETECTORS = ["latency_log", "failure_rate", "rerun_rate", "union", "two_of_three", "mewma"]
LABELS = ["MAD latency", "MAD failure", "MAD rerun", "MAD union", "MAD two-of-three", "MEWMA"]
COLORS = ["#0072B2", "#D55E00", "#009E73", "#CC79A7", "#8B6F23", "#222222"]
MARKERS = ["o", "s", "^", "D", "v", "P"]
STYLES = ["-", "--", ":", "-.", "--", "-"]
CASES = {"docker": ("docker/cli", "Docker"), "tekton": ("tektoncd/pipeline", "Tekton")}


def holdout_cases(weekly):
    cases = {}
    for key, (repo, _) in CASES.items():
        rows = weekly.loc[(weekly.repo_full == repo) & (weekly.source_split == "holdout")].copy()
        rows["week"] = pd.to_datetime(rows.week, format="%Y-%m-%d")
        rows = rows.set_index(["week", "detector_variant"]).sort_index()
        dates = pd.date_range("2026-03-30", "2026-08-03", freq="W-MON")
        expected = pd.MultiIndex.from_product([dates, DETECTORS])
        if not rows.index.is_unique or not rows.index.sort_values().equals(expected.sort_values()):
            raise ValueError(f"Incomplete weekly detector grid for {repo}")
        q = rows.xs("mewma", level="detector_variant")
        if q.frozen_control_limit.nunique() != 1 or not np.array_equal(
            q.alarm, q.raw_mewma_stat > q.frozen_control_limit
        ):
            raise ValueError(f"Stored MEWMA decisions disagree with the frozen limit for {repo}")
        if not np.array_equal(rows.evaluable, rows.alarm.notna()):
            raise ValueError(f"Stored alarm availability disagrees with evaluability for {repo}")
        cases[key] = rows
    return cases


def top_legend(ax, columns, size=9):
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.005), ncol=columns,
              fontsize=size, frameon=False, borderaxespad=0, columnspacing=1.2)


def trajectory(plt, case, rows):
    from matplotlib.colors import BoundaryNorm, ListedColormap
    from matplotlib.ticker import MaxNLocator

    q = rows.xs("mewma", level="detector_variant")
    dates = q.index
    x = np.arange(len(dates))
    fig, axes = plt.subplots(4, 1, figsize=(7.0, 8.5), sharex=True, layout="constrained",
                             gridspec_kw={"height_ratios": [1.1, 1.2, 1.05, 0.8]})
    fig.suptitle(f"{CASES[case][1]} observed holdout, 30 March–3 August 2026")
    limit = q.frozen_control_limit.iloc[0]
    axes[0].plot(x, q.raw_mewma_stat, "-o", color=COLORS[-1], ms=3, label="Stored MEWMA statistic Q")
    axes[0].axhline(limit, color="#B53636", ls="--", label=f"h = {limit:.3f}")
    axes[0].set(ylabel="MEWMA Q", ylim=(0, 40))
    top_legend(axes[0], 2)
    for i, detector in enumerate(DETECTORS[:3]):
        component = rows.xs(detector, level="detector_variant")
        axes[1].plot(x, component.magnitude, marker=MARKERS[i], ls=STYLES[i],
                     color=COLORS[i], ms=3, label=LABELS[i])
    axes[1].axhline(3, color="#777777", ls="--", lw=0.8, label="Raw threshold")
    axes[1].set(ylabel="Component\nscaled-MAD score", ylim=(-4, 20))
    top_legend(axes[1], 4, 8)

    alarms = rows.alarm.unstack().loc[dates, DETECTORS].astype(float).fillna(-1).to_numpy().T
    cmap = ListedColormap(["#bcbcbc", "#ffffff", "#216b9a"])
    axes[2].imshow(alarms, origin="upper", aspect="auto", interpolation="nearest", cmap=cmap,
                   norm=BoundaryNorm([-1.5, -0.5, 0.5, 1.5], cmap.N))
    axes[2].set_yticks(range(6), ["Latency", "Failure", "Rerun", "Union", "Two of three", "MEWMA"])
    axes[2].set_ylabel("Stored decisions")
    axes[2].set_title("Blue = alarm     White = defined non-alarm     Grey = unavailable", fontsize=9)

    count = axes[3].plot(x, q.workflow_count, "-s", ms=3, color=COLORS[2], label="Workflow count")
    axes[3].set_ylabel("Workflows")
    axes[3].yaxis.set_major_locator(MaxNLocator(integer=True))
    right = axes[3].twinx()
    hhi = right.plot(x, q.workflow_concentration_hhi, ":D", ms=3, color="#7b3294", label="Workflow HHI")
    right.set(ylabel="Workflow HHI", ylim=(0, 1))
    axes[3].legend(handles=count + hhi, loc="lower center", bbox_to_anchor=(0.5, 1.005),
                   ncol=2, frameon=False, fontsize=9, borderaxespad=0)
    for ax in axes:
        ax.set_xlim(-0.5, len(dates) - 0.5)
        ax.grid(axis="y", alpha=0.18)
    axes[3].set_xticks(x[::3], dates[::3].strftime("%d %b"))
    axes[3].set_xlabel("Monday week label (2026)")
    return fig


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path,
                        default=ROOT / "analysis_outputs" / "rq1_baseline_week_results.csv")
    parser.add_argument("--out-dir", type=Path, default=ROOT / "Figures")
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    # Recorded in the RQ1 baseline validation status manifest.
    expected_hash = "bac4965c3af6f5f7d41586553760d19c26b9ac419ec77af5de90bf5fb349644c"
    if not args.input.is_file():
        parser.error(f"Could not find {args.input}. Pass the study CSV with --input.")
    if hashlib.sha256(args.input.read_bytes()).hexdigest() != expected_hash:
        parser.error("The input differs from the frozen RQ1 weekly export used by the thesis.")
    cases = holdout_cases(pd.read_csv(args.input))
    for case, rows in cases.items():
        q = rows.xs("mewma", level="detector_variant")
        print(f"{CASES[case][1]}: {int(q.alarm.sum())} MEWMA alarm weeks out of {len(q)}")
    if args.check_only:
        return
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10})
    args.out_dir.mkdir(parents=True, exist_ok=True)
    for case, rows in cases.items():
        fig = trajectory(plt, case, rows)
        path = args.out_dir / f"rq1_{case}_trajectory.png"
        fig.savefig(path, format="png", dpi=300, bbox_inches="tight")
        plt.close(fig)
        print(path)


if __name__ == "__main__":
    main()
