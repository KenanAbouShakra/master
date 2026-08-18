from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from ci_common import (
    CORE_METRICS,
    load_config,
    output_dir,
    robust_training_standardize,
)


OUTPUT_NAMES = (
    "baseline_results.csv",
    "mad_summary_long.csv",
    "mewma_summary.csv",
    "optimal_partitioning_robustness_change_points.csv",
    "baseline_metadata.json",
)

MINIMUM_SEGMENT_LENGTH = 2


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_measurement_freeze(root: Path, out: Path) -> tuple[str, dict]:
    manifest_path = out / "MEASUREMENT_FROZEN.json"
    if not manifest_path.is_file():
        raise RuntimeError(
            f"Measurement freeze manifest is absent: {manifest_path}"
        )

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Invalid measurement freeze manifest: {exc}"
        ) from exc

    required = {
        "manifest_version",
        "freeze",
        "files",
        "methodology",
        "repositories",
        "external_calibration_declaration",
        "modelling_declaration",
    }
    missing = sorted(required - set(manifest))
    if missing:
        raise RuntimeError(
            "Measurement freeze manifest is missing fields: "
            + ", ".join(missing)
        )

    files = manifest["files"]
    if not isinstance(files, dict) or not files:
        raise RuntimeError("Measurement freeze manifest has no file records")

    failures: list[str] = []
    for relative, record in files.items():
        if not isinstance(record, dict) or not {
            "sha256",
            "bytes",
        }.issubset(record):
            failures.append(f"{relative}: invalid record")
            continue

        path = root / relative
        if not path.is_file():
            failures.append(f"{relative}: missing")
        elif path.stat().st_size != int(record["bytes"]):
            failures.append(f"{relative}: byte size differs")
        elif file_sha256(path) != str(record["sha256"]):
            failures.append(f"{relative}: SHA-256 differs")

    if failures:
        raise RuntimeError(
            "Measurement freeze validation failed: "
            + "; ".join(failures)
        )

    return file_sha256(manifest_path), manifest


def validate_output_paths(
    root: Path,
    out: Path,
    manifest: dict,
    output_names: tuple[str, ...] = OUTPUT_NAMES,
) -> dict:
    if len(output_names) != len(set(output_names)):
        raise RuntimeError("Step-03 output names contain duplicates")

    resolved_output_directory = out.resolve()
    frozen_paths = {
        (root / relative).resolve()
        for relative in manifest["files"]
    }
    intended_paths: set[Path] = set()

    for name in output_names:
        candidate_name = Path(name)
        if (
            candidate_name.is_absolute()
            or candidate_name.name != name
            or candidate_name.parent != Path(".")
        ):
            raise RuntimeError(
                f"Step-03 output name is not a plain filename: {name}"
            )

        intended = (resolved_output_directory / name).resolve()
        if intended.parent != resolved_output_directory:
            raise RuntimeError(
                f"Step-03 output is outside the output directory: {name}"
            )
        if intended in frozen_paths:
            raise RuntimeError(
                f"Step-03 output collides with a frozen file: {name}"
            )
        intended_paths.add(intended)

    return {
        "status": "PASS",
        "protected_frozen_path_count": len(frozen_paths),
        "intended_output_count": len(intended_paths),
        "output_directory": str(resolved_output_directory),
        "collision_count": 0,
        "plain_filenames_only": True,
        "duplicate_names": False,
        "all_outputs_within_output_directory": True,
    }


def atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".tmp",
        dir=path.parent,
        encoding="utf-8",
        newline="",
        delete=False,
    )
    temporary = Path(handle.name)
    handle.close()
    try:
        frame.to_csv(
            temporary,
            index=False,
            date_format="%Y-%m-%d",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_json(payload: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".tmp",
        dir=path.parent,
        encoding="utf-8",
        delete=False,
    )
    temporary = Path(handle.name)
    try:
        json.dump(
            payload,
            handle,
            indent=2,
            ensure_ascii=False,
            allow_nan=False,
        )
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        os.replace(temporary, path)
    except Exception:
        if not handle.closed:
            handle.close()
        raise
    finally:
        temporary.unlink(missing_ok=True)


def causal_mad(
    values: pd.Series,
    window: int,
    threshold: float,
) -> tuple[pd.Series, pd.Series]:
    numeric = pd.to_numeric(values, errors="coerce")
    history = numeric.shift(1).rolling(window, min_periods=window)
    center = history.median()
    mad = history.apply(
        lambda observations: float(
            np.median(
                np.abs(observations - np.median(observations))
            )
        ),
        raw=True,
    )
    scale = 1.4826 * mad
    score = (numeric - center) / scale.where(scale.gt(0))
    alarm = score.gt(threshold).where(score.notna()).astype("boolean")
    return score, alarm


def persistence(flags: pd.Series, weeks: int) -> pd.Series:
    boolean = flags.astype("boolean")
    known = (
        boolean.notna()
        .rolling(weeks, min_periods=weeks)
        .sum()
        .eq(weeks)
    )
    hits = (
        boolean.fillna(False)
        .astype(int)
        .rolling(weeks, min_periods=weeks)
        .sum()
        .eq(weeks)
    )
    return hits.where(known).astype("boolean")


def scoring_partition(frame: pd.DataFrame) -> pd.Series:
    required = {
        "split",
        "external_calibration",
        "external_evaluation_eligible",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(
            "Panel is missing scoring columns: " + ", ".join(missing)
        )

    external = frame["split"].eq("external")
    calibration = frame["external_calibration"].fillna(False).astype(bool)
    evaluation = (
        frame["external_evaluation_eligible"]
        .fillna(False)
        .astype(bool)
    )

    if (calibration & evaluation).any():
        raise ValueError(
            "External calibration and evaluation flags overlap"
        )

    partition = frame["split"].astype("string").copy()
    partition.loc[external & calibration] = "external_calibration"
    partition.loc[external & evaluation] = "external_evaluation"
    partition.loc[
        external & ~calibration & ~evaluation
    ] = "external_unscored"
    return partition


def episode_statistics(flags: pd.Series) -> dict[str, int | float | None]:
    durations: list[int] = []
    current = 0
    for value in flags.astype("boolean"):
        if pd.isna(value) or not bool(value):
            if current:
                durations.append(current)
                current = 0
        else:
            current += 1
    if current:
        durations.append(current)

    return {
        "episode_count": len(durations),
        "total_episode_weeks": int(sum(durations)),
        "median_episode_duration_weeks": (
            float(np.median(durations)) if durations else None
        ),
        "maximum_episode_duration_weeks": (
            int(max(durations)) if durations else None
        ),
    }


def summarize_alarm(
    flags: pd.Series,
    magnitude: pd.Series,
    *,
    repo: str,
    split: str,
    variant: str,
    metric: str,
    window: int,
    threshold: float,
    persistence_weeks: int,
    cfg: dict,
) -> dict:
    evaluable = flags.notna()
    alarms = flags.astype("boolean").fillna(False)
    alarm_magnitude = pd.to_numeric(
        magnitude.where(alarms),
        errors="coerce",
    ).dropna()

    return {
        "repo_full": repo,
        "split": split,
        "detector_family": "causal_rolling_mad",
        "detector_variant": variant,
        "metric": metric,
        "window_weeks": window,
        "window_role": (
            "primary"
            if window == int(cfg["mad"]["primary_window_weeks"])
            else "sensitivity"
        ),
        "threshold": threshold,
        "threshold_role": (
            "primary"
            if threshold == float(cfg["mad"]["primary_threshold"])
            else "sensitivity"
        ),
        "persistence_weeks": persistence_weeks,
        "persistence_role": (
            "primary"
            if persistence_weeks
            == int(cfg["mad"]["primary_persistence_weeks"])
            else "sensitivity"
        ),
        "total_weeks": int(len(flags)),
        "evaluable_weeks": int(evaluable.sum()),
        "excluded_weeks": int((~evaluable).sum()),
        "alarm_weeks": int(alarms.sum()),
        "alarm_prevalence": (
            float(alarms[evaluable].mean()) if evaluable.any() else None
        ),
        **episode_statistics(flags),
        "median_alarm_magnitude": (
            float(alarm_magnitude.median())
            if not alarm_magnitude.empty
            else None
        ),
        "maximum_alarm_magnitude": (
            float(alarm_magnitude.max())
            if not alarm_magnitude.empty
            else None
        ),
        "external_scoring_eligible": split == "external_evaluation",
        "interpretation": "detector signal, not ground truth",
    }


def fit_mad(
    panel: pd.DataFrame,
    cfg: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    result = panel.sort_values(["repo_full", "week"]).copy()
    result["reporting_partition"] = scoring_partition(result)

    windows = {
        int(cfg["mad"]["primary_window_weeks"]),
        *map(int, cfg["mad"]["sensitivity_windows_weeks"]),
    }
    summaries: list[dict] = []

    for window in sorted(windows):
        for threshold_value in cfg["mad"]["thresholds"]:
            threshold = float(threshold_value)
            component_columns: list[str] = []
            score_columns: list[str] = []

            for metric in CORE_METRICS:
                alarm_column = (
                    f"mad_w{window}_t{threshold:g}_{metric}"
                )
                score_column = alarm_column + "_score"
                result[alarm_column] = pd.Series(
                    pd.NA,
                    index=result.index,
                    dtype="boolean",
                )
                result[score_column] = np.nan

                for indexes in result.groupby(
                    "repo_full",
                    sort=False,
                ).groups.values():
                    score, alarm = causal_mad(
                        result.loc[indexes, metric],
                        window,
                        threshold,
                    )
                    result.loc[indexes, alarm_column] = (
                        alarm.astype("boolean").array
                    )
                    result.loc[indexes, score_column] = score.array

                component_columns.append(alarm_column)
                score_columns.append(score_column)

            all_components_evaluable = (
                result[component_columns].notna().all(axis=1)
            )
            hits = (
                result[component_columns]
                .astype("boolean")
                .fillna(False)
                .astype(int)
                .sum(axis=1)
            )
            union = (
                hits.ge(1)
                .where(all_components_evaluable)
                .astype("boolean")
            )
            two_of_three = (
                hits.ge(2)
                .where(all_components_evaluable)
                .astype("boolean")
            )
            composite_magnitude = result[score_columns].max(
                axis=1,
                skipna=False,
            )

            variants = [
                *[
                    (
                        metric,
                        metric,
                        result[alarm_column],
                        result[score_column],
                    )
                    for metric, alarm_column, score_column in zip(
                        CORE_METRICS,
                        component_columns,
                        score_columns,
                    )
                ],
                ("union", "composite", union, composite_magnitude),
                (
                    "two_of_three",
                    "composite",
                    two_of_three,
                    composite_magnitude,
                ),
            ]

            for variant, metric, raw_alarm, magnitude in variants:
                for persistence_value in cfg["mad"]["persistence_weeks"]:
                    persistence_weeks = int(persistence_value)
                    output_column = (
                        f"mad_w{window}_t{threshold:g}_"
                        f"{variant}_k{persistence_weeks}"
                    )
                    result[output_column] = pd.Series(
                        pd.NA,
                        index=result.index,
                        dtype="boolean",
                    )

                    for indexes in result.groupby(
                        "repo_full",
                        sort=False,
                    ).groups.values():
                        result.loc[indexes, output_column] = persistence(
                            raw_alarm.loc[indexes],
                            persistence_weeks,
                        ).array

                    grouped = result.groupby(
                        ["repo_full", "reporting_partition"],
                        sort=True,
                    ).groups
                    for (repo, split), indexes in grouped.items():
                        split = str(split)
                        if split in {
                            "external_calibration",
                            "external_unscored",
                            "outside",
                        }:
                            continue

                        summaries.append(
                            summarize_alarm(
                                result.loc[indexes, output_column],
                                magnitude.loc[indexes],
                                repo=str(repo),
                                split=split,
                                variant=variant,
                                metric=metric,
                                window=window,
                                threshold=threshold,
                                persistence_weeks=persistence_weeks,
                                cfg=cfg,
                            )
                        )

    return result, pd.DataFrame(summaries)


def fit_mewma(
    panel: pd.DataFrame,
    cfg: dict,
) -> tuple[pd.DataFrame, dict, pd.DataFrame]:
    result, standardization = robust_training_standardize(panel, cfg)
    if "reporting_partition" not in result.columns:
        result["reporting_partition"] = scoring_partition(result)

    lam = float(cfg["mewma"]["lambda"])
    quantile = float(cfg["mewma"]["calibration_quantile"])
    if not 0 < lam <= 1:
        raise ValueError("MEWMA lambda must be in (0, 1]")
    if not 0 < quantile < 1:
        raise ValueError("MEWMA calibration quantile must be in (0, 1)")

    training_statistics: list[float] = []
    z_columns = [f"z_{metric}" for metric in CORE_METRICS]

    for indexes in result.groupby(
        "repo_full",
        sort=False,
    ).groups.values():
        z_values = (
            result.loc[indexes, z_columns]
            .clip(lower=-8, upper=8)
            .to_numpy(float)
        )
        ewma = np.full_like(z_values, np.nan)
        previous = np.zeros(len(CORE_METRICS), dtype=float)

        for position, row in enumerate(z_values):
            observed = np.isfinite(row)
            if not observed.any():
                continue
            previous[observed] = (
                lam * row[observed]
                + (1 - lam) * previous[observed]
            )
            ewma[position] = previous

        statistic = np.full(len(ewma), np.nan)
        evaluable = np.isfinite(ewma).any(axis=1)
        statistic[evaluable] = np.sum(
            np.square(np.maximum(ewma[evaluable], 0)),
            axis=1,
        )
        result.loc[indexes, "mewma_stat"] = statistic

        training = result.loc[indexes, "split"].eq("train").to_numpy()
        training_statistics.extend(
            statistic[training & np.isfinite(statistic)].tolist()
        )

    if not training_statistics:
        raise RuntimeError(
            "No evaluable development-training MEWMA statistics"
        )

    limit = float(np.quantile(training_statistics, quantile))
    if not np.isfinite(limit):
        raise RuntimeError("MEWMA empirical control limit is not finite")

    result["mewma_limit"] = limit
    result["mewma_alarm"] = (
        result["mewma_stat"]
        .gt(limit)
        .where(result["mewma_stat"].notna())
        .astype("boolean")
    )

    summaries: list[dict] = []
    grouped = result.groupby(
        ["repo_full", "reporting_partition"],
        sort=True,
    ).groups
    for (repo, split), indexes in grouped.items():
        split = str(split)
        if split in {
            "external_calibration",
            "external_unscored",
            "outside",
        }:
            continue

        flags = result.loc[indexes, "mewma_alarm"]
        evaluable = flags.notna()
        alarms = flags.astype("boolean").fillna(False)
        summaries.append(
            {
                "repo_full": str(repo),
                "split": split,
                "detector_family": "mewma",
                "total_weeks": int(len(flags)),
                "evaluable_weeks": int(evaluable.sum()),
                "excluded_weeks": int((~evaluable).sum()),
                "alarm_weeks": int(alarms.sum()),
                "alarm_prevalence": (
                    float(alarms[evaluable].mean())
                    if evaluable.any()
                    else None
                ),
                "external_scoring_eligible": (
                    split == "external_evaluation"
                ),
                "interpretation": "detector signal, not ground truth",
            }
        )

    metadata = {
        "lambda": lam,
        "lambda_role": "fixed_from_frozen_configuration",
        "calibration_quantile": quantile,
        "empirical_limit": limit,
        "control_limit_reference": "development_training_rows_only",
        "training_statistics_used": len(training_statistics),
        "all_missing_policy": "statistic_and_alarm_remain_missing",
        "standardization": standardization,
    }
    return result, metadata, pd.DataFrame(summaries)


def optimal_partitioning_change_points(
    values: np.ndarray,
    penalty: float,
) -> tuple[list[int], int]:
    numeric = np.asarray(values, dtype=float)
    finite = np.isfinite(numeric)
    missing_count = int((~finite).sum())
    if finite.sum() < 8:
        return [], missing_count

    filled = (
        pd.Series(numeric)
        .interpolate(limit_direction="both")
        .to_numpy()
    )
    length = len(filled)
    prefix = np.r_[0.0, np.cumsum(filled)]
    prefix_squared = np.r_[0.0, np.cumsum(filled**2)]

    def segment_cost(start: int, end: int) -> float:
        segment_length = end - start
        segment_sum = prefix[end] - prefix[start]
        segment_square_sum = (
            prefix_squared[end] - prefix_squared[start]
        )
        return float(
            segment_square_sum
            - segment_sum**2 / max(segment_length, 1)
        )

    best = np.full(length + 1, np.inf)
    previous = np.full(length + 1, -1, dtype=int)
    best[0] = -penalty

    for end in range(MINIMUM_SEGMENT_LENGTH, length + 1):
        candidates = np.arange(
            0,
            end - MINIMUM_SEGMENT_LENGTH + 1,
        )
        candidates = candidates[np.isfinite(best[candidates])]
        if not len(candidates):
            continue
        scores = np.array(
            [
                best[start]
                + segment_cost(start, end)
                + penalty
                for start in candidates
            ]
        )
        selected = int(np.argmin(scores))
        best[end] = scores[selected]
        previous[end] = int(candidates[selected])

    points: list[int] = []
    cursor = length
    while previous[cursor] > 0:
        cursor = int(previous[cursor])
        points.append(cursor)

    boundaries = [0, *sorted(points), length]
    if any(
        end - start < MINIMUM_SEGMENT_LENGTH
        for start, end in zip(boundaries, boundaries[1:])
    ):
        raise AssertionError(
            "Optimal partitioning produced an undersized segment"
        )

    return sorted(points), missing_count


def build_change_point_table(
    panel: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    rows: list[dict] = []
    sequences = 0
    for repo, group in panel.groupby("repo_full", sort=False):
        training = (
            group.loc[group["split"].eq("train")]
            .sort_values("week")
            .reset_index(drop=True)
        )
        if training.empty:
            continue

        penalty = float(3 * np.log(max(len(training), 2)))
        for metric in CORE_METRICS:
            sequences += 1
            points, missing_count = optimal_partitioning_change_points(
                training[metric].to_numpy(float),
                penalty,
            )
            for point in points:
                rows.append(
                    {
                        "repo_full": str(repo),
                        "split": "train",
                        "metric": metric,
                        "week": training.iloc[point]["week"],
                        "index": int(point),
                        "penalty": penalty,
                        "sequence_weeks": int(len(training)),
                        "interpolated_missing_values": missing_count,
                        "algorithm": (
                            "exact_unpruned_optimal_partitioning"
                        ),
                        "minimum_segment_length": (
                            MINIMUM_SEGMENT_LENGTH
                        ),
                        "pruning": False,
                        "role": "robustness_analysis_only",
                        "interpretation": "not ground truth",
                    }
                )

    columns = [
        "repo_full",
        "split",
        "metric",
        "week",
        "index",
        "penalty",
        "sequence_weeks",
        "interpolated_missing_values",
        "algorithm",
        "minimum_segment_length",
        "pruning",
        "role",
        "interpretation",
    ]
    return (
        pd.DataFrame(rows, columns=columns),
        {
            "algorithm": "exact_unpruned_optimal_partitioning",
            "minimum_segment_length": MINIMUM_SEGMENT_LENGTH,
            "pruning": False,
            "role": "robustness_analysis_only",
            "ground_truth": False,
            "penalty": "3 * natural_log(sequence_length)",
            "minimum_finite_observations": 8,
            "missingness": "linear_interpolation_with_boundary_fill",
            "sequences_evaluated": sequences,
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="analysis_config.yaml")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    root = config_path.parent
    cfg = load_config(config_path)
    out = output_dir(cfg, config_path)

    manifest_sha256, manifest = validate_measurement_freeze(root, out)
    output_protection = validate_output_paths(root, out, manifest)

    quality_path = out / "data_quality_report.json"
    quality_report = json.loads(quality_path.read_text(encoding="utf-8"))
    if quality_report.get("status") != "PASS":
        raise RuntimeError("Quality gate is not PASS")

    panel_path = out / "repository_week_panel_audited.csv"
    panel = pd.read_csv(panel_path, parse_dates=["week"])
    if panel.empty:
        raise RuntimeError("Audited repository-week panel is empty")

    expected_repositories = {
        *cfg["study"]["development_repositories"],
        *cfg["study"]["external_repositories"],
    }
    observed_repositories = set(panel["repo_full"].dropna().astype(str))
    if observed_repositories != expected_repositories:
        raise RuntimeError(
            "Panel repositories differ from the frozen configuration"
        )

    baseline_panel, mad_summary = fit_mad(panel, cfg)
    baseline_panel, mewma_metadata, mewma_summary = fit_mewma(
        baseline_panel,
        cfg,
    )
    change_points, change_point_metadata = build_change_point_table(
        baseline_panel
    )

    outputs = {
        "baseline_results.csv": baseline_panel,
        "mad_summary_long.csv": mad_summary,
        "mewma_summary.csv": mewma_summary,
        "optimal_partitioning_robustness_change_points.csv": (
            change_points
        ),
    }

    metadata = {
        "measurement_freeze": {
            "manifest": "analysis_outputs/MEASUREMENT_FROZEN.json",
            "manifest_sha256": manifest_sha256,
            "manifest_version": manifest["manifest_version"],
            "validated_files": len(manifest["files"]),
        },
        "output_protection": output_protection,
        "quality_status": quality_report["status"],
        "input": {
            "panel": "analysis_outputs/repository_week_panel_audited.csv",
            "panel_sha256": file_sha256(panel_path),
            "rows": int(len(panel)),
            "repositories": sorted(observed_repositories),
        },
        "mad": {
            "family": "causal_rolling_mad",
            "current_week_excluded": True,
            "primary_window_weeks": int(
                cfg["mad"]["primary_window_weeks"]
            ),
            "sensitivity_windows_weeks": [
                int(value)
                for value in cfg["mad"]["sensitivity_windows_weeks"]
            ],
            "thresholds": [
                float(value) for value in cfg["mad"]["thresholds"]
            ],
            "primary_threshold": float(
                cfg["mad"]["primary_threshold"]
            ),
            "persistence_weeks": [
                int(value)
                for value in cfg["mad"]["persistence_weeks"]
            ],
            "primary_persistence_weeks": int(
                cfg["mad"]["primary_persistence_weeks"]
            ),
            "zero_mad_policy": "unevaluable",
            "incomplete_warmup_policy": "unevaluable",
            "composite_support": "all_three_components_required",
            "interpretation": "detector signals, not ground truth",
        },
        "mewma": mewma_metadata,
        "change_point_robustness": change_point_metadata,
        "external_evaluation": {
            "calibration_weeks_excluded_from_scoring": True,
            "eligible_flag": "external_evaluation_eligible",
        },
        "outputs": {
            name: {
                "rows": int(len(frame)),
                "role": (
                    "modelling_output_not_measurement_artifact"
                ),
            }
            for name, frame in outputs.items()
        },
        "interpretation": {
            "alarms_are_ground_truth": False,
            "change_points_are_ground_truth": False,
        },
    }

    for name, frame in outputs.items():
        atomic_csv(frame, out / name)
    atomic_json(metadata, out / "baseline_metadata.json")

    created = sorted([*outputs, "baseline_metadata.json"])
    if tuple(created) != tuple(sorted(OUTPUT_NAMES)):
        raise AssertionError("Written outputs differ from OUTPUT_NAMES")

    print(
        json.dumps(
            {
                "status": "PASS",
                "measurement_manifest_sha256": manifest_sha256,
                "baseline_rows": int(len(baseline_panel)),
                "mad_summary_rows": int(len(mad_summary)),
                "mewma_summary_rows": int(len(mewma_summary)),
                "change_point_rows": int(len(change_points)),
                "outputs": created,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())