"""Plot Chapter 5 RQ2 figures from retained numerical exports in V2.

Run from the repository root:
    python V2/plot_chapter5_rq2_exports.py
    python V2/plot_chapter5_rq2_exports.py --figure all --check-only

Use --data-dir and --out-dir to override the default input and output folders.
The modelling panel is not a figure input; plots use the verified RQ2 exports.
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
DETECTORS = ["MEWMA", "MAD latency", "MAD union", "MAD two-of-three", "MAD failure", "MAD rerun"]
COLORS = ["#0173b2", "#de8f05", "#029e73", "#d55e00", "#cc78bc", "#ca9161"]
FILES = {
    "tradeoff": ("operating_tradeoff", "figure_rq2_1_detection_reference_alarm_burden_thesis.png"),
    "repository": ("repository", "rq2_repository_thesis.png"),
    "duration": ("duration", "rq2_duration_thesis.png"),
    "magnitude": ("magnitude", "rq2_magnitude_thesis.png"),
    "missingness": ("controlled_missingness", "rq2_missingness_pooled.png"),
}
TRADE_LABELS = ["Strict incremental\ndetection (%)", "Mean reference\nalarm burden (%)",
               "Mean paired-week\nunavailability (%)"]


def values_for(key, directory):
    source, _ = FILES[key]
    data = pd.read_csv(directory / f"figure_data_{source}.csv")
    if key == "tradeoff":
        data = data.set_index("detector").loc[DETECTORS]
        if not data.planned_scenarios.eq(25200).all():
            raise ValueError("Unexpected number of primary scenarios")
        np.testing.assert_allclose(data.detection_percent,
                                   100 * data.strict_detection_scenarios / data.planned_scenarios)
        # Burden is exported as a fraction; the other two fields are already percentages.
        values = pd.DataFrame({
            TRADE_LABELS[0]: data.detection_percent,
            TRADE_LABELS[1]: 100 * data.reference_alarm_burden_scenario_mean,
            TRADE_LABELS[2]: data.mean_pair_week_unavailability_percent,
        })
    elif key == "missingness":
        summary = data[data.row_type.eq("detector_summary")].set_index("detector")
        values = summary.loc[DETECTORS, ["difference_percentage_points"]]
        design = data[data.row_type.eq("design_cell")].groupby("detector")
        np.testing.assert_allclose(values.iloc[:, 0], design.difference_percentage_points.mean().loc[DETECTORS])
    else:
        column, levels = {
            "repository": ("repo_full", ["containerd/containerd", "helm/helm", "pytest-dev/pytest"]),
            "duration": ("duration_weeks", [1, 2, 4, 8]),
            "magnitude": ("magnitude_level", ["low", "medium", "high"]),
        }[key]
        np.testing.assert_allclose(data.detection_percent,
                                   100 * data.strict_detection_scenarios / data.planned_scenarios)
        values = data.pivot(index="detector", columns=column, values="detection_percent").loc[DETECTORS, levels]
    if values.shape[0] != len(DETECTORS) or not np.isfinite(values.to_numpy()).all():
        raise ValueError(f"Invalid plot values for {key}")
    return values


def draw(plt, key, values):
    if key in ("tradeoff", "missingness"):
        fig, axes = plt.subplots(1, len(values.columns), sharey=True, squeeze=False,
                                 figsize=(9, 4.5), layout="constrained")
        for ax, (label, series) in zip(axes[0], values.items()):
            bars = ax.barh(DETECTORS, series, color=COLORS, height=.65)
            ax.bar_label(bars, fmt="%.2f", padding=3, fontsize=8)
            if key == "tradeoff":
                ax.set_title(label, fontsize=11)
                ax.set_xlim(0, 62)
            else:
                ax.set_xlim(min(0, float(series.min())) - 6, max(0, float(series.max())) + 6)
                ax.axvline(0, color="0.4", linewidth=.8)
                ax.set_xlabel("Controlled missingness minus observed detection (percentage points)")
                ax.set_title("Detection difference at medium magnitude")
        axes[0, 0].invert_yaxis()
    else:
        fig, ax = plt.subplots(figsize=(8.5, 4.5), layout="constrained")
        if key == "repository":
            x = np.arange(len(values.columns))
            for i, detector in enumerate(DETECTORS):
                ax.bar(x + (i - 2.5) * .14, values.loc[detector], width=.135, color=COLORS[i], label=detector)
            ax.set_xticks(x, list(values.columns))
        else:
            x = list(values.columns) if key == "duration" else [0, 1, 2]
            for i, detector in enumerate(DETECTORS):
                ax.plot(x, values.loc[detector], marker=["o", "s", "^", "D", "v", "P"][i],
                        color=COLORS[i], label=detector)
            ax.set_xticks(x, [str(v).capitalize() for v in values.columns])
            ax.set_xlabel("Deviation duration (weeks)" if key == "duration" else "Specified magnitude")
        ax.set_ylim(0, 100)
        ax.set_ylabel("Strict incremental detection (%)")
        ax.legend(loc="lower center", bbox_to_anchor=(.5, 1), ncol=3, frameon=False, fontsize=9)
    return fig


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--figure", choices=[*FILES, "all"], default="all")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "analysis_outputs" / "rq2_stage_b_visualizations_chapter5_panels_v2")
    parser.add_argument("--out-dir", type=Path, default=ROOT.parent / "figures")
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    for key in FILES if args.figure == "all" else [args.figure]:
        values = values_for(key, args.data_dir)
        path = args.out_dir / FILES[key][1]
        print(path)
        print(values.round(2).to_string())
        if args.check_only:
            continue
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        args.out_dir.mkdir(parents=True, exist_ok=True)
        fig = draw(plt, key, values)
        fig.savefig(path, dpi=300, bbox_inches="tight", facecolor="white")
        plt.close(fig)


if __name__ == "__main__":
    main()
