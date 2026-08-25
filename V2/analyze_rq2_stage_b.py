from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd


RESULTS_MANIFEST_SHA256 = "3edd5fb241ba6828b0db44ba80b9316a71693f3467b3990c22b671d2dfe6d4e7"
EXPECTED_ROWS = 252018
GROUP_KEYS = [
    "repo_full", "detector_family", "detector_variant", "scenario_type",
    "affected_metrics", "dimensionality", "magnitude_level",
    "duration_weeks", "volume_condition", "missingness_condition",
]
BINARY_METRICS = [
    "strict_incremental_episode_detected",
    "total_operational_episode_detected",
    "incremental_detected_within_0w_post",
    "incremental_detected_within_1w_post",
    "incremental_detected_within_2w_post",
    "incremental_detected_within_4w_post",
]
CONTINUOUS_METRICS = [
    "strict_incremental_detection_delay_weeks",
    "reference_alarm_burden",
    "incremental_alarm_duration_weeks",
    "incremental_spillover_weeks",
    "reference_alarm_overlap_fraction",
    "incremental_boundary_overlap",
    "pair_unevaluable_fraction",
    "injection_relative_precision",
    "injection_relative_false_alarm_rate",
]
OUTPUTS = (
    "rq2_operating_profiles.csv",
    "rq2_magnitude_profiles.csv",
    "rq2_duration_profiles.csv",
    "rq2_dimensionality_profiles.csv",
    "rq2_data_quality_contrasts.csv",
    "rq2_cross_repository_stability.csv",
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


def atomic_json(value: dict, path: Path) -> None:
    handle = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    )
    temporary = Path(handle.name)
    try:
        json.dump(value, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        os.replace(temporary, path)
    finally:
        if not handle.closed:
            handle.close()
        temporary.unlink(missing_ok=True)


def verify_results_freeze(root: Path) -> dict:
    manifest_path = root / "analysis_outputs" / "RQ2_STAGE_B_RESULTS_FROZEN.json"
    if not manifest_path.is_file():
        raise RuntimeError("RQ2 Stage-B results freeze is missing")
    actual_manifest_hash = sha256(manifest_path)
    if actual_manifest_hash != RESULTS_MANIFEST_SHA256:
        raise RuntimeError("RQ2 Stage-B results freeze hash differs")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "FROZEN":
        raise RuntimeError("RQ2 Stage-B results are not marked FROZEN")
    failures = {}
    for relative, record in manifest.get("files", {}).items():
        path = root / relative
        if not path.is_file():
            failures[relative] = "missing"
        elif path.stat().st_size != int(record["bytes"]):
            failures[relative] = "size mismatch"
        elif sha256(path) != str(record["sha256"]):
            failures[relative] = "hash mismatch"
    if failures:
        raise RuntimeError("Frozen Stage-B result mismatch: " + json.dumps(failures))
    return {
        "path": str(manifest_path.relative_to(root)),
        "sha256": actual_manifest_hash,
        "files_verified": len(manifest.get("files", {})),
    }


def bool_numeric(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        return values.astype(float)
    mapped = values.astype("string").str.lower().map({"true": 1.0, "false": 0.0})
    numeric = pd.to_numeric(values, errors="coerce")
    return mapped.fillna(numeric)


def wilson(successes: float, n: int, z: float = 1.959963984540054) -> tuple[float | None, float | None]:
    if n <= 0:
        return None, None
    p = successes / n
    denominator = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denominator
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denominator
    return max(0.0, center - half), min(1.0, center + half)


def summarize_group(group: pd.DataFrame) -> dict:
    row: dict = {"scenario_detector_rows": int(len(group))}
    for metric in BINARY_METRICS:
        values = bool_numeric(group[metric]).dropna()
        n = int(len(values))
        successes = float(values.sum())
        low, high = wilson(successes, n)
        row.update({
            f"{metric}_n": n,
            f"{metric}_proportion": successes / n if n else None,
            f"{metric}_ci95_low": low,
            f"{metric}_ci95_high": high,
        })
    for metric in CONTINUOUS_METRICS:
        values = pd.to_numeric(group[metric], errors="coerce").dropna().to_numpy(float)
        n = int(len(values))
        mean = float(np.mean(values)) if n else None
        sd = float(np.std(values, ddof=1)) if n > 1 else None
        half = 1.959963984540054 * sd / math.sqrt(n) if n > 1 else None
        row.update({
            f"{metric}_n": n,
            f"{metric}_mean": mean,
            f"{metric}_sd": sd,
            f"{metric}_ci95_low": mean - half if half is not None else None,
            f"{metric}_ci95_high": mean + half if half is not None else None,
        })
    return row


def grouped_profiles(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    rows = []
    for key, group in frame.groupby(keys, dropna=False, sort=True):
        if not isinstance(key, tuple):
            key = (key,)
        rows.append({**dict(zip(keys, key)), **summarize_group(group)})
    return pd.DataFrame(rows)


def quality_contrasts(frame: pd.DataFrame) -> pd.DataFrame:
    injected = frame.loc[frame["scenario_type"].eq("metric_shift")].copy()
    medium = injected.loc[injected["magnitude_level"].eq("medium")].copy()
    design_keys = [
        "repo_full", "detector_family", "detector_variant", "affected_metrics",
        "dimensionality", "duration_weeks",
    ]
    metrics = BINARY_METRICS + CONTINUOUS_METRICS
    observed = medium.loc[
        medium["volume_condition"].eq("observed")
        & medium["missingness_condition"].eq("none")
    ]
    rows = []
    for label, mask in {
        "low_volume_minus_observed": medium["volume_condition"].eq("low_volume"),
        "controlled_missingness_minus_observed": medium["missingness_condition"].eq("controlled"),
    }.items():
        altered = medium.loc[mask]
        for key, left_group in altered.groupby(design_keys, sort=True):
            selector = pd.Series(True, index=observed.index)
            for column, value in zip(design_keys, key):
                selector &= observed[column].eq(value)
            right_group = observed.loc[selector]
            base = dict(zip(design_keys, key))
            for metric in metrics:
                left = bool_numeric(left_group[metric]) if metric in BINARY_METRICS else pd.to_numeric(left_group[metric], errors="coerce")
                right = bool_numeric(right_group[metric]) if metric in BINARY_METRICS else pd.to_numeric(right_group[metric], errors="coerce")
                left = left.dropna().to_numpy(float)
                right = right.dropna().to_numpy(float)
                difference = float(np.mean(left) - np.mean(right)) if len(left) and len(right) else None
                variance = (
                    (np.var(left, ddof=1) / len(left) if len(left) > 1 else 0.0)
                    + (np.var(right, ddof=1) / len(right) if len(right) > 1 else 0.0)
                )
                half = 1.959963984540054 * math.sqrt(variance) if difference is not None else None
                rows.append({
                    **base, "contrast": label, "metric": metric,
                    "altered_scenarios": int(len(left)),
                    "observed_scenarios": int(len(right)),
                    "difference_in_means": difference,
                    "ci95_low": difference - half if half is not None else None,
                    "ci95_high": difference + half if half is not None else None,
                    "interpretation": "design-cell simulation contrast; injection starts differ across condition profiles; not paired and not a real-world causal effect",
                })
    return pd.DataFrame(rows)


def cross_repository_stability(operating: pd.DataFrame) -> pd.DataFrame:
    condition_keys = [key for key in GROUP_KEYS if key != "repo_full"]
    metrics = [
        "strict_incremental_episode_detected_proportion",
        "strict_incremental_detection_delay_weeks_mean",
        "reference_alarm_burden_mean",
        "incremental_spillover_weeks_mean",
        "pair_unevaluable_fraction_mean",
    ]
    rows = []
    for key, group in operating.groupby(condition_keys, dropna=False, sort=True):
        base = dict(zip(condition_keys, key))
        for metric in metrics:
            values = pd.to_numeric(group[metric], errors="coerce").dropna().to_numpy(float)
            rows.append({
                **base, "metric": metric, "repositories_observed": int(len(values)),
                "cross_repository_mean": float(np.mean(values)) if len(values) else None,
                "cross_repository_minimum": float(np.min(values)) if len(values) else None,
                "cross_repository_maximum": float(np.max(values)) if len(values) else None,
                "cross_repository_range": float(np.ptp(values)) if len(values) else None,
                "cross_repository_sd": float(np.std(values, ddof=1)) if len(values) > 1 else None,
                "interpretation": "descriptive stability across three repositories; not population-level transportability",
            })
    return pd.DataFrame(rows)


def reference_burden(frame: pd.DataFrame) -> pd.DataFrame:
    keys = ["repo_full", "detector_family", "detector_variant"]
    rows = []
    for key, group in frame.groupby(keys, sort=True):
        values = pd.to_numeric(group["reference_alarm_burden"], errors="coerce").dropna()
        unique = np.sort(values.unique())
        rows.append({
            **dict(zip(keys, key)), "scenario_detector_rows": int(len(group)),
            "reference_alarm_burden": float(values.mean()) if len(values) else None,
            "unique_reference_burden_values": int(len(unique)),
            "minimum": float(unique.min()) if len(unique) else None,
            "maximum": float(unique.max()) if len(unique) else None,
            "interpretation": "alarm burden on unaltered external series; not a false-alarm rate",
        })
    return pd.DataFrame(rows)


def preflight(root: Path) -> tuple[pd.DataFrame, dict]:
    freeze = verify_results_freeze(root)
    output = root / "analysis_outputs" / "rq2_stage_b"
    detector_path = output / "rq2_stage_b_detector_results.csv.gz"
    frame = pd.read_csv(detector_path, low_memory=False)
    required = set(GROUP_KEYS + BINARY_METRICS + CONTINUOUS_METRICS + ["scenario_id", "detector_id", "repetition"])
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError("Detector results missing columns: " + ", ".join(missing))
    if len(frame) != EXPECTED_ROWS:
        raise RuntimeError(f"Detector row count differs: {len(frame)}")
    analysis_dir = root / "analysis_outputs" / "rq2_stage_b_analysis"
    plan = {
        "status": "PASS",
        "results_freeze": freeze,
        "detector_rows": int(len(frame)),
        "scenario_ids": int(frame["scenario_id"].nunique()),
        "repositories": sorted(frame["repo_full"].dropna().astype(str).unique()),
        "detectors": sorted(frame["detector_id"].dropna().astype(str).unique()),
        "magnitude_levels": sorted(frame.loc[frame["scenario_type"].eq("metric_shift"), "magnitude_level"].dropna().astype(str).unique()),
        "durations_weeks": sorted(pd.to_numeric(frame.loc[frame["scenario_type"].eq("metric_shift"), "duration_weeks"], errors="coerce").dropna().astype(int).unique().tolist()),
        "dimensionalities": sorted(pd.to_numeric(frame.loc[frame["scenario_type"].eq("metric_shift"), "dimensionality"], errors="coerce").dropna().astype(int).unique().tolist()),
        "volume_conditions": sorted(frame["volume_condition"].dropna().astype(str).unique()),
        "missingness_conditions": sorted(frame["missingness_condition"].dropna().astype(str).unique()),
        "analysis_output_directory_exists": analysis_dir.exists(),
        "derived_outputs_written": False,
        "claim_scope": "controlled perturbation responsiveness; not real-world ground truth or causal effects",
    }
    if analysis_dir.exists():
        raise RuntimeError(f"Analysis output directory already exists: {analysis_dir}")
    return frame, plan


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preflight-only", action="store_true")
    mode.add_argument("--confirm-analysis", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    frame, plan = preflight(root)
    print("RQ2 STAGE-B ANALYSIS PREFLIGHT")
    print(json.dumps(plan, indent=2))
    if args.preflight_only:
        return 0

    injected = frame.loc[frame["scenario_type"].eq("metric_shift")].copy()
    operating = grouped_profiles(injected, GROUP_KEYS)
    magnitude = grouped_profiles(
        injected.loc[injected["volume_condition"].eq("observed") & injected["missingness_condition"].eq("none")],
        ["repo_full", "detector_family", "detector_variant", "affected_metrics", "dimensionality", "magnitude_level"],
    )
    duration = grouped_profiles(
        injected.loc[injected["volume_condition"].eq("observed") & injected["missingness_condition"].eq("none")],
        ["repo_full", "detector_family", "detector_variant", "affected_metrics", "dimensionality", "duration_weeks"],
    )
    dimensionality = grouped_profiles(
        injected.loc[injected["volume_condition"].eq("observed") & injected["missingness_condition"].eq("none")],
        ["repo_full", "detector_family", "detector_variant", "dimensionality", "magnitude_level", "duration_weeks"],
    )
    contrasts = quality_contrasts(frame)
    stability = cross_repository_stability(operating)
    burden = reference_burden(frame)
    tables = {
        OUTPUTS[0]: operating,
        OUTPUTS[1]: magnitude,
        OUTPUTS[2]: duration,
        OUTPUTS[3]: dimensionality,
        OUTPUTS[4]: contrasts,
        OUTPUTS[5]: stability,
        OUTPUTS[6]: burden,
    }
    analysis_dir = root / "analysis_outputs" / "rq2_stage_b_analysis"
    analysis_dir.mkdir(parents=True, exist_ok=False)
    try:
        for name, table in tables.items():
            atomic_csv(table, analysis_dir / name)
        metadata = {
            "status": "PASS",
            "source_results_manifest_sha256": RESULTS_MANIFEST_SHA256,
            "source_detector_rows": int(len(frame)),
            "methods": {
                "binary_uncertainty": "Wilson 95% interval",
                "continuous_uncertainty": "scenario-level mean plus normal-approximation 95% Monte Carlo interval",
                "delay_denominator": "detected scenarios only; always reported with detection proportion",
                "data_quality_comparisons": "difference in means within matched design cells at medium magnitude; injection starts are independently seeded across condition profiles",
                "cross_repository_stability": "descriptive range and SD across three repositories",
            },
            "interpretation_limits": {
                "universal_winner": False,
                "real_world_ground_truth": False,
                "causal_effect": False,
                "population_transportability": False,
            },
            "outputs": {name: {"rows": int(len(table)), "sha256": sha256(analysis_dir / name)} for name, table in tables.items()},
        }
        atomic_json(metadata, analysis_dir / OUTPUTS[7])
    except Exception:
        for path in analysis_dir.iterdir():
            path.unlink(missing_ok=True)
        analysis_dir.rmdir()
        raise
    print("RQ2 STAGE-B DERIVED ANALYSIS")
    print(json.dumps({
        "status": "PASS", "output_directory": str(analysis_dir),
        "outputs": {name: int(len(table)) for name, table in tables.items()},
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
