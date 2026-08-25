from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import tempfile
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


DETECTOR_ORDER = [
    "MEWMA",
    "MAD latency",
    "MAD union",
    "MAD two-of-three",
    "MAD failure",
    "MAD rerun",
]
DETECTOR_LABELS = {
    ("mewma", "mewma"): "MEWMA",
    ("causal_rolling_mad", "latency_log"): "MAD latency",
    ("causal_rolling_mad", "union"): "MAD union",
    ("causal_rolling_mad", "two_of_three"): "MAD two-of-three",
    ("causal_rolling_mad", "failure_rate"): "MAD failure",
    ("causal_rolling_mad", "rerun_rate"): "MAD rerun",
}
COLORS = dict(zip(DETECTOR_ORDER, sns.color_palette("colorblind", len(DETECTOR_ORDER))))
EXPECTED_FILES = (
    "rq2_operating_profiles.csv",
    "rq2_magnitude_profiles.csv",
    "rq2_duration_profiles.csv",
    "rq2_data_quality_contrasts.csv",
    "rq2_reference_alarm_burden.csv",
    "rq2_analysis_metadata.json",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    handle = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    )
    temporary = Path(handle.name)
    handle.close()
    try:
        frame.to_csv(temporary, index=False, lineterminator="\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def detector_label(frame: pd.DataFrame) -> pd.Series:
    labels = []
    for family, variant in zip(frame["detector_family"], frame["detector_variant"]):
        key = (str(family), str(variant))
        if key not in DETECTOR_LABELS:
            raise ValueError(f"Unknown detector identity: {key}")
        labels.append(DETECTOR_LABELS[key])
    return pd.Series(labels, index=frame.index, dtype="string")


def wilson(successes: float, n: float, z: float = 1.959963984540054) -> tuple[float, float]:
    if n <= 0:
        return np.nan, np.nan
    p = successes / n
    denominator = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denominator
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denominator
    return max(0.0, center - half), min(1.0, center + half)


def aggregate_binary(frame: pd.DataFrame, group_keys: list[str], stem: str) -> pd.DataFrame:
    proportion = f"{stem}_proportion"
    denominator = f"{stem}_n"
    rows = []
    for key, group in frame.groupby(group_keys, sort=True, dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        n = pd.to_numeric(group[denominator], errors="coerce").fillna(0).sum()
        successes = (
            pd.to_numeric(group[proportion], errors="coerce")
            * pd.to_numeric(group[denominator], errors="coerce")
        ).fillna(0).sum()
        estimate = successes / n if n else np.nan
        low, high = wilson(float(successes), float(n))
        rows.append({
            **dict(zip(group_keys, key)),
            "estimate": estimate,
            "ci95_low": low,
            "ci95_high": high,
            "n": int(n),
        })
    return pd.DataFrame(rows)


def weighted_mean(frame: pd.DataFrame, value: str, weight: str, group_keys: list[str]) -> pd.DataFrame:
    rows = []
    for key, group in frame.groupby(group_keys, sort=True, dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        values = pd.to_numeric(group[value], errors="coerce")
        weights = pd.to_numeric(group[weight], errors="coerce")
        valid = values.notna() & weights.notna() & weights.gt(0)
        mean = np.average(values[valid], weights=weights[valid]) if valid.any() else np.nan
        rows.append({**dict(zip(group_keys, key)), "estimate": mean, "weight": int(weights[valid].sum())})
    return pd.DataFrame(rows)


def save_figure(fig: plt.Figure, output: Path, stem: str) -> list[str]:
    names = []
    for suffix, dpi in (("png", 300), ("pdf", None)):
        path = output / f"{stem}.{suffix}"
        fig.savefig(path, dpi=dpi, bbox_inches="tight", facecolor="white")
        names.append(path.name)
    plt.close(fig)
    return names


def prepare(input_dir: Path) -> dict[str, pd.DataFrame]:
    missing = [name for name in EXPECTED_FILES if not (input_dir / name).is_file()]
    if missing:
        raise FileNotFoundError("Missing RQ2 analysis files: " + ", ".join(missing))
    metadata = json.loads((input_dir / "rq2_analysis_metadata.json").read_text(encoding="utf-8"))
    if metadata.get("status") != "PASS":
        raise RuntimeError("RQ2 analysis metadata status is not PASS")
    for name, record in metadata["outputs"].items():
        path = input_dir / name
        if path.is_file() and sha256(path) != record["sha256"]:
            raise RuntimeError(f"Analysis input hash differs: {name}")
    tables = {
        "operating": pd.read_csv(input_dir / "rq2_operating_profiles.csv", low_memory=False),
        "magnitude": pd.read_csv(input_dir / "rq2_magnitude_profiles.csv", low_memory=False),
        "duration": pd.read_csv(input_dir / "rq2_duration_profiles.csv", low_memory=False),
        "quality": pd.read_csv(input_dir / "rq2_data_quality_contrasts.csv", low_memory=False),
        "burden": pd.read_csv(input_dir / "rq2_reference_alarm_burden.csv", low_memory=False),
    }
    for table in tables.values():
        table["detector"] = detector_label(table)
    return tables


def primary_operating(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.loc[
        frame["scenario_type"].eq("metric_shift")
        & frame["volume_condition"].eq("observed")
        & frame["missingness_condition"].eq("none")
    ].copy()


def figure_pareto(primary: pd.DataFrame, burden: pd.DataFrame, output: Path) -> tuple[list[str], pd.DataFrame]:
    detection = aggregate_binary(
        primary, ["detector"], "strict_incremental_episode_detected"
    )
    burden_summary = weighted_mean(
        primary, "reference_alarm_burden_mean", "reference_alarm_burden_n", ["detector"]
    ).rename(columns={"estimate": "reference_alarm_burden"})
    unevaluable = weighted_mean(
        primary, "pair_unevaluable_fraction_mean", "pair_unevaluable_fraction_n", ["detector"]
    ).rename(columns={"estimate": "unevaluable_fraction"})
    data = detection.merge(burden_summary[["detector", "reference_alarm_burden"]], on="detector").merge(
        unevaluable[["detector", "unevaluable_fraction"]], on="detector"
    )
    fig, ax = plt.subplots(figsize=(8.4, 5.8))
    offsets = {
        "MEWMA": (6, 5), "MAD latency": (6, 5), "MAD union": (6, 5),
        "MAD two-of-three": (6, 5), "MAD failure": (8, 10), "MAD rerun": (8, -2),
    }
    for _, row in data.iterrows():
        ax.scatter(
            row["reference_alarm_burden"] * 100,
            row["estimate"] * 100,
            s=90 + row["unevaluable_fraction"] * 500,
            color=COLORS[row["detector"]], edgecolor="black", linewidth=0.6, zorder=3,
        )
        ax.annotate(row["detector"], (row["reference_alarm_burden"] * 100, row["estimate"] * 100),
                    xytext=offsets[row["detector"]], textcoords="offset points", fontsize=9)
    ax.set_xlabel("Reference alarm burden (%)")
    ax.set_ylabel("Strict incremental episode detection (%)")
    ax.set_title("Detector operating profiles under the primary data condition")
    ax.text(0.01, -0.19, "Point size represents the unevaluable fraction. Reference alarms are not labelled false alarms.",
            transform=ax.transAxes, fontsize=8.5)
    ax.grid(alpha=0.25)
    sns.despine(ax=ax)
    return save_figure(fig, output, "figure_rq2_1_operating_tradeoff"), data


def line_profile(frame: pd.DataFrame, x: str, xlabel: str, title: str, stem: str, output: Path) -> tuple[list[str], pd.DataFrame]:
    data = aggregate_binary(frame, ["detector", x], "strict_incremental_episode_detected")
    fig, ax = plt.subplots(figsize=(9.0, 5.8))
    for detector in DETECTOR_ORDER:
        group = data.loc[data["detector"].eq(detector)].sort_values(x)
        ax.plot(group[x], group["estimate"] * 100, marker="o", linewidth=2, label=detector, color=COLORS[detector])
        ax.fill_between(group[x], group["ci95_low"] * 100, group["ci95_high"] * 100,
                        color=COLORS[detector], alpha=0.10)
    ax.set_xlabel(xlabel)
    if x == "magnitude_order":
        ax.set_xticks([1, 2, 3], ["Low", "Medium", "High"])
    ax.set_ylabel("Strict incremental episode detection (%)")
    ax.set_title(title)
    ax.set_ylim(bottom=0)
    ax.grid(alpha=0.25)
    ax.legend(frameon=False, ncol=2, fontsize=8.5)
    sns.despine(ax=ax)
    return save_figure(fig, output, stem), data


def figure_repository(primary: pd.DataFrame, output: Path) -> tuple[list[str], pd.DataFrame]:
    data = aggregate_binary(primary, ["repo_full", "detector"], "strict_incremental_episode_detected")
    repositories = sorted(data["repo_full"].unique())
    x = np.arange(len(repositories))
    width = 0.12
    fig, ax = plt.subplots(figsize=(11.0, 6.0))
    for index, detector in enumerate(DETECTOR_ORDER):
        group = data.loc[data["detector"].eq(detector)].set_index("repo_full").reindex(repositories)
        positions = x + (index - 2.5) * width
        ax.bar(positions, group["estimate"] * 100, width, label=detector, color=COLORS[detector])
    ax.set_xticks(x, [repo.replace("/", "/\n") for repo in repositories])
    ax.set_ylabel("Strict incremental episode detection (%)")
    ax.set_title("Repository-specific detector responsiveness")
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", alpha=0.25)
    ax.legend(frameon=False, ncol=3, fontsize=8.5)
    sns.despine(ax=ax)
    return save_figure(fig, output, "figure_rq2_4_repository_responsiveness"), data


def figure_missingness(quality: pd.DataFrame, output: Path) -> tuple[list[str], pd.DataFrame]:
    data = quality.loc[
        quality["contrast"].eq("controlled_missingness_minus_observed")
        & quality["metric"].eq("strict_incremental_episode_detected")
    ].copy()
    rows = []
    for detector, group in data.groupby("detector", sort=False):
        weights = pd.to_numeric(group["altered_scenarios"], errors="coerce")
        values = pd.to_numeric(group["difference_in_means"], errors="coerce")
        valid = weights.gt(0) & values.notna()
        rows.append({
            "detector": detector,
            "difference_percentage_points": float(np.average(values[valid], weights=weights[valid]) * 100),
            "design_cells": int(valid.sum()),
        })
    summary = pd.DataFrame(rows).set_index("detector").reindex(DETECTOR_ORDER).reset_index()
    fig, ax = plt.subplots(figsize=(8.8, 5.6))
    ax.barh(summary["detector"], summary["difference_percentage_points"],
            color=[COLORS[item] for item in summary["detector"]])
    ax.axvline(0, color="black", linewidth=0.8)
    ax.set_xlabel("Change in strict detection (percentage points)")
    ax.set_title("Descriptive contrast: controlled missingness versus observed data")
    ax.text(0.01, -0.16, "Matched design-cell contrast at medium magnitude; independently seeded injection locations; not causal.",
            transform=ax.transAxes, fontsize=8.5)
    ax.grid(axis="x", alpha=0.25)
    sns.despine(ax=ax)
    return save_figure(fig, output, "figure_rq2_5_missingness_contrast"), summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="analysis_outputs/rq2_stage_b_analysis")
    parser.add_argument("--output-dir", default="analysis_outputs/rq2_stage_b_visualizations")
    parser.add_argument("--confirm-write", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    input_dir = (root / args.input_dir).resolve()
    output = (root / args.output_dir).resolve()
    tables = prepare(input_dir)
    primary = primary_operating(tables["operating"])
    plan = {
        "status": "PASS",
        "primary_profile_rows": int(len(primary)),
        "detectors": DETECTOR_ORDER,
        "figures_planned": 5,
        "formats": ["png", "pdf"],
        "terminology": "controlled/injected CI deviation; alarm; operating characteristics",
        "invalid_continuous_normal_intervals_plotted": False,
        "output_directory_exists": output.exists(),
        "outputs_written": False,
    }
    print("RQ2 VISUALIZATION PLAN")
    print(json.dumps(plan, indent=2))
    if not args.confirm_write:
        return 0
    if output.exists():
        raise RuntimeError(f"Refusing to overwrite visualization directory: {output}")
    output.mkdir(parents=True, exist_ok=False)
    sns.set_theme(context="paper", style="whitegrid", font_scale=1.1)
    created: list[str] = []
    try:
        names, tradeoff = figure_pareto(primary, tables["burden"], output)
        created.extend(names)
        names, duration = line_profile(
            tables["duration"], "duration_weeks", "Injected deviation duration (weeks)",
            "Responsiveness increases with deviation duration",
            "figure_rq2_2_detection_by_duration", output,
        )
        created.extend(names)
        magnitude_map = {"low": 1, "medium": 2, "high": 3}
        magnitude_source = tables["magnitude"].copy()
        magnitude_source["magnitude_order"] = magnitude_source["magnitude_level"].map(magnitude_map)
        names, magnitude = line_profile(
            magnitude_source, "magnitude_order", "Injected deviation magnitude",
            "Responsiveness increases with deviation magnitude",
            "figure_rq2_3_detection_by_magnitude", output,
        )
        created.extend(names)
        names, repository = figure_repository(primary, output)
        created.extend(names)
        names, missingness = figure_missingness(tables["quality"], output)
        created.extend(names)
        tables_to_write = {
            "figure_data_operating_tradeoff.csv": tradeoff,
            "figure_data_detection_by_duration.csv": duration,
            "figure_data_detection_by_magnitude.csv": magnitude,
            "figure_data_repository_responsiveness.csv": repository,
            "figure_data_missingness_contrast.csv": missingness,
        }
        for name, frame in tables_to_write.items():
            atomic_csv(frame, output / name)
            created.append(name)
        manifest = {
            "status": "PASS",
            "source_analysis_metadata_sha256": sha256(input_dir / "rq2_analysis_metadata.json"),
            "point_estimates_only_for_continuous_metrics": True,
            "binary_intervals": "reconstructed Wilson 95% intervals from aggregated successes and denominators",
            "terminology": "controlled/injected CI deviation; never operational degradation",
            "files": {name: {"sha256": sha256(output / name), "bytes": (output / name).stat().st_size} for name in created},
        }
        manifest_path = output / "rq2_visualization_manifest.json"
        handle = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output, delete=False)
        temporary = Path(handle.name)
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.close()
        os.replace(temporary, manifest_path)
        created.append(manifest_path.name)
    except Exception:
        for path in output.iterdir():
            path.unlink(missing_ok=True)
        output.rmdir()
        raise
    print("RQ2 VISUALIZATIONS")
    print(json.dumps({"status": "PASS", "output_directory": str(output), "files": created}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())