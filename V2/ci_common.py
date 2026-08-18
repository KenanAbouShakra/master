from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd
import yaml


CORE_METRICS = ("latency_log", "failure_rate", "rerun_rate")


def load_config(path: str | Path) -> dict:
    """Load and minimally validate the analysis configuration."""

    config_path = Path(path)

    with config_path.open(encoding="utf-8-sig") as handle:
        cfg = yaml.safe_load(handle)

    if not isinstance(cfg, dict):
        raise ValueError("Configuration root must be a mapping")

    required_sections = {"study", "paths"}
    missing_sections = sorted(required_sections - set(cfg))

    if missing_sections:
        raise ValueError(
            "Configuration is missing sections: "
            + ", ".join(missing_sections)
        )

    study = cfg["study"]

    required_study_fields = {
        "development_repositories",
        "external_repositories",
        "external_calibration_weeks",
        "analysis_start",
        "analysis_end",
        "train_end",
        "purge_start",
        "purge_end",
        "holdout_start",
        "holdout_end",
    }

    missing_study_fields = sorted(required_study_fields - set(study))

    if missing_study_fields:
        raise ValueError(
            "Configuration study section is missing fields: "
            + ", ".join(missing_study_fields)
        )

    calibration_weeks = int(study["external_calibration_weeks"])

    if calibration_weeks <= 0:
        raise ValueError(
            "study.external_calibration_weeks must be a positive integer"
        )

    development = list(study["development_repositories"])
    external = list(study["external_repositories"])

    overlap = sorted(set(development) & set(external))

    if overlap:
        raise ValueError(
            "Repositories cannot be both development and external: "
            + ", ".join(overlap)
        )

    return cfg


def repositories(cfg: dict) -> list[str]:
    """Return the frozen development and external repository sample."""

    study = cfg["study"]

    return (
        list(study["development_repositories"])
        + list(study["external_repositories"])
    )


def output_dir(cfg: dict, config_path: str | Path) -> Path:
    """Resolve and create the configured analysis-output directory."""

    root = Path(config_path).resolve().parent
    value = Path(cfg["paths"]["output"])
    path = value if value.is_absolute() else root / value

    path.mkdir(parents=True, exist_ok=True)

    return path


def sqlite_path(cfg: dict, config_path: str | Path) -> Path:
    """Resolve the configured SQLite database path."""

    root = Path(config_path).resolve().parent
    value = Path(cfg["paths"]["sqlite"])

    return value if value.is_absolute() else root / value


def sha256(path: str | Path) -> str:
    """Calculate the SHA-256 digest of a file."""

    digest = hashlib.sha256()

    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def write_json(path: str | Path, payload: dict | list) -> None:
    """Write a JSON artifact using stable UTF-8 formatting."""

    Path(path).write_text(
        json.dumps(
            payload,
            indent=2,
            ensure_ascii=False,
            allow_nan=False,
        ),
        encoding="utf-8",
    )


def require_columns(
    frame: pd.DataFrame,
    columns: Iterable[str],
    label: str,
) -> None:
    """Raise an explicit error if required columns are absent."""

    missing = sorted(set(columns) - set(frame.columns))

    if missing:
        raise ValueError(
            f"{label} is missing columns: {', '.join(missing)}"
        )


def monday(values: pd.Series) -> pd.Series:
    """Convert timestamps to timezone-naive Monday week identifiers."""

    parsed = pd.to_datetime(
        values,
        errors="coerce",
        utc=True,
    ).dt.tz_convert(None)

    return (
        parsed
        - pd.to_timedelta(parsed.dt.weekday, unit="D")
    ).dt.normalize()


def _config_date(value: object, label: str) -> pd.Timestamp:
    """Parse a configuration date as a normalized naive timestamp."""

    parsed = pd.to_datetime(value, errors="coerce")

    if pd.isna(parsed):
        raise ValueError(f"Invalid configuration date for {label}: {value}")

    timestamp = pd.Timestamp(parsed)

    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(None)

    return timestamp.normalize()


def add_external_calibration_flags(
    panel: pd.DataFrame,
    cfg: dict,
) -> pd.DataFrame:
    """
    Mark the frozen external calibration prefix and scored evaluation weeks.

    For each external repository, the first N observed analysis weeks are
    calibration weeks. An observed week must have attempts_total > 0.

    Calibration weeks are never externally scored. Subsequent observed
    analysis weeks are marked as externally evaluation-eligible.
    """

    require_columns(
        panel,
        ("repo_full", "week", "attempts_total"),
        "repository-week panel",
    )

    result = panel.copy()
    result["week"] = monday(result["week"])

    if result["week"].isna().any():
        invalid_count = int(result["week"].isna().sum())
        raise ValueError(
            f"Repository-week panel contains {invalid_count} invalid weeks"
        )

    study = cfg["study"]
    external_repositories = list(study["external_repositories"])
    external_set = set(external_repositories)
    prefix_weeks = int(study["external_calibration_weeks"])

    if prefix_weeks <= 0:
        raise ValueError(
            "study.external_calibration_weeks must be positive"
        )

    analysis_start = _config_date(
        study["analysis_start"],
        "study.analysis_start",
    )
    analysis_end = _config_date(
        study["analysis_end"],
        "study.analysis_end",
    )

    if analysis_end < analysis_start:
        raise ValueError(
            "study.analysis_end must not precede study.analysis_start"
        )

    attempts = pd.to_numeric(
        result["attempts_total"],
        errors="coerce",
    )

    observed = attempts.fillna(0).gt(0)
    in_analysis_period = result["week"].between(
        analysis_start,
        analysis_end,
        inclusive="both",
    )

    result["external_calibration"] = False
    result["external_evaluation_eligible"] = False

    calibration_summary: dict[str, dict[str, object]] = {}

    for repo in external_repositories:
        repo_rows = result["repo_full"].eq(repo)

        if not repo_rows.any():
            raise ValueError(
                f"External repository is absent from the panel: {repo}"
            )

        observed_weeks = (
            result.loc[
                repo_rows & observed & in_analysis_period,
                "week",
            ]
            .drop_duplicates()
            .sort_values()
            .reset_index(drop=True)
        )

        if len(observed_weeks) < prefix_weeks:
            raise ValueError(
                f"{repo} has only {len(observed_weeks)} observed "
                f"analysis weeks; {prefix_weeks} are required for "
                "external calibration"
            )

        calibration_weeks = observed_weeks.iloc[:prefix_weeks]
        calibration_week_set = set(calibration_weeks.tolist())

        calibration_mask = (
            repo_rows
            & in_analysis_period
            & result["week"].isin(calibration_week_set)
        )

        evaluation_mask = (
            repo_rows
            & in_analysis_period
            & observed
            & ~result["week"].isin(calibration_week_set)
        )

        result.loc[
            calibration_mask,
            "external_calibration",
        ] = True

        result.loc[
            evaluation_mask,
            "external_evaluation_eligible",
        ] = True

        selected_count = int(
            result.loc[
                repo_rows & result["external_calibration"],
                "week",
            ].nunique()
        )

        if selected_count != prefix_weeks:
            raise AssertionError(
                f"{repo} received {selected_count} calibration weeks; "
                f"expected exactly {prefix_weeks}"
            )

        overlap_count = int(
            (
                result.loc[repo_rows, "external_calibration"]
                & result.loc[
                    repo_rows,
                    "external_evaluation_eligible",
                ]
            ).sum()
        )

        if overlap_count:
            raise AssertionError(
                f"{repo} has {overlap_count} weeks marked as both "
                "calibration and evaluation"
            )

        calibration_summary[repo] = {
            "calibration_weeks": selected_count,
            "first_calibration_week": (
                calibration_weeks.iloc[0].strftime("%Y-%m-%d")
            ),
            "last_calibration_week": (
                calibration_weeks.iloc[-1].strftime("%Y-%m-%d")
            ),
            "evaluation_weeks": int(
                result.loc[
                    repo_rows
                    & result["external_evaluation_eligible"],
                    "week",
                ].nunique()
            ),
        }

    development_rows = ~result["repo_full"].isin(external_set)

    if result.loc[
        development_rows,
        "external_calibration",
    ].any():
        raise AssertionError(
            "Development repositories were incorrectly marked as "
            "external calibration"
        )

    if result.loc[
        development_rows,
        "external_evaluation_eligible",
    ].any():
        raise AssertionError(
            "Development repositories were incorrectly marked as "
            "external evaluation"
        )

    result.attrs["external_calibration_summary"] = calibration_summary

    return result


def _robust_location_scale(
    values: pd.Series,
    repo: str,
    metric: str,
) -> tuple[float, float, str]:
    """
    Estimate a robust median and scale from a frozen reference sample.

    MAD is primary. Sample standard deviation is used only when MAD is
    zero or numerically unusable. A metric with no estimable reference
    scale causes an explicit quality failure rather than silent filling.
    """

    numeric = pd.to_numeric(
        values,
        errors="coerce",
    ).replace([np.inf, -np.inf], np.nan).dropna()

    if numeric.empty:
        raise ValueError(
            f"No valid calibration observations for "
            f"repository={repo}, metric={metric}"
        )

    median = float(numeric.median())
    mad = float(np.median(np.abs(numeric.to_numpy() - median)))
    scale = 1.4826 * mad
    scale_method = "scaled_mad"

    if not np.isfinite(scale) or scale <= 1e-12:
        scale = (
            float(numeric.std(ddof=1))
            if len(numeric) > 1
            else np.nan
        )
        scale_method = "sample_standard_deviation_fallback"

    if not np.isfinite(scale) or scale <= 1e-12:
        raise ValueError(
            "Cannot estimate a positive calibration scale for "
            f"repository={repo}, metric={metric}, "
            f"valid_observations={len(numeric)}"
        )

    return median, scale, scale_method


def robust_training_standardize(
    panel: pd.DataFrame,
    cfg: dict,
    metrics: Sequence[str] = CORE_METRICS,
) -> tuple[pd.DataFrame, dict]:
    """
    Standardize repository-week metrics without evaluation leakage.

    Development repositories:
        Reference = rows assigned to the training split.

    External repositories:
        Reference = the frozen first N observed analysis weeks.

    The estimated parameters are then applied to all rows for that
    repository. External post-calibration observations cannot influence
    their calibration parameters, and external observations never
    influence development parameters.
    """

    required = {
        "repo_full",
        "week",
        "attempts_total",
        "split",
        *metrics,
    }

    require_columns(
        panel,
        required,
        "repository-week panel",
    )

    result = panel.copy()
    result["week"] = monday(result["week"])

    if (
        "external_calibration" not in result.columns
        or "external_evaluation_eligible" not in result.columns
    ):
        result = add_external_calibration_flags(result, cfg)

    external_repositories = set(
        cfg["study"]["external_repositories"]
    )

    prefix_weeks = int(
        cfg["study"]["external_calibration_weeks"]
    )

    parameters: dict[str, dict[str, object]] = {}

    grouped_indexes = result.groupby(
        "repo_full",
        sort=False,
    ).groups

    for repo, indexes in grouped_indexes.items():
        repository = result.loc[indexes]

        if repo in external_repositories:
            reference_mask = repository[
                "external_calibration"
            ].fillna(False).astype(bool)

            reference_label = (
                f"external_first_observed_{prefix_weeks}_weeks"
            )

            reference_week_count = int(
                repository.loc[
                    reference_mask,
                    "week",
                ].nunique()
            )

            if reference_week_count != prefix_weeks:
                raise ValueError(
                    f"{repo} has {reference_week_count} external "
                    f"calibration weeks; expected {prefix_weeks}"
                )
        else:
            reference_mask = repository["split"].eq("train")
            reference_label = "development_training_split"

            reference_week_count = int(
                repository.loc[
                    reference_mask,
                    "week",
                ].nunique()
            )

            if reference_week_count == 0:
                raise ValueError(
                    f"{repo} has no development-training weeks"
                )

        calibration = repository.loc[reference_mask]

        parameters[repo] = {
            "reference": reference_label,
            "reference_weeks": reference_week_count,
            "metrics": {},
        }

        for metric in metrics:
            median, scale, scale_method = _robust_location_scale(
                calibration[metric],
                repo=repo,
                metric=metric,
            )

            valid_reference_observations = int(
                pd.to_numeric(
                    calibration[metric],
                    errors="coerce",
                )
                .replace([np.inf, -np.inf], np.nan)
                .notna()
                .sum()
            )

            parameters[repo]["metrics"][metric] = {
                "median": median,
                "scale": scale,
                "scale_method": scale_method,
                "reference_observations": (
                    valid_reference_observations
                ),
            }

            all_values = pd.to_numeric(
                result.loc[indexes, metric],
                errors="coerce",
            ).replace([np.inf, -np.inf], np.nan)

            result.loc[indexes, f"z_{metric}"] = (
                all_values - median
            ) / scale

    expected_repositories = set(repositories(cfg))
    parameter_repositories = set(parameters)

    missing_parameters = sorted(
        expected_repositories - parameter_repositories
    )

    if missing_parameters:
        raise ValueError(
            "No standardization parameters were created for: "
            + ", ".join(missing_parameters)
        )

    unexpected_repositories = sorted(
        parameter_repositories - expected_repositories
    )

    if unexpected_repositories:
        raise ValueError(
            "Panel contains repositories outside the frozen sample: "
            + ", ".join(unexpected_repositories)
        )

    return result, parameters


def assign_split(
    weeks: pd.Series,
    repos: pd.Series,
    cfg: dict,
) -> pd.Series:
    """Assign frozen development and external analysis splits."""

    dates = monday(weeks)
    study = cfg["study"]

    external_repositories = set(
        study["external_repositories"]
    )

    development_repositories = set(
        study["development_repositories"]
    )

    known_repositories = (
        external_repositories | development_repositories
    )

    unknown = sorted(
        set(repos.dropna().astype(str)) - known_repositories
    )

    if unknown:
        raise ValueError(
            "Cannot assign split to repositories outside the "
            "frozen sample: "
            + ", ".join(unknown)
        )

    analysis_start = _config_date(
        study["analysis_start"],
        "study.analysis_start",
    )
    analysis_end = _config_date(
        study["analysis_end"],
        "study.analysis_end",
    )
    train_end = _config_date(
        study["train_end"],
        "study.train_end",
    )
    purge_start = _config_date(
        study["purge_start"],
        "study.purge_start",
    )
    purge_end = _config_date(
        study["purge_end"],
        "study.purge_end",
    )
    holdout_start = _config_date(
        study["holdout_start"],
        "study.holdout_start",
    )
    holdout_end = _config_date(
        study["holdout_end"],
        "study.holdout_end",
    )

    split = pd.Series(
        "outside",
        index=weeks.index,
        dtype="string",
    )

    external = repos.isin(external_repositories)
    development = repos.isin(development_repositories)

    in_analysis_period = dates.between(
        analysis_start,
        analysis_end,
        inclusive="both",
    )

    split.loc[external & in_analysis_period] = "external"

    split.loc[
        development
        & dates.between(
            analysis_start,
            train_end,
            inclusive="both",
        )
    ] = "train"

    split.loc[
        development
        & dates.between(
            purge_start,
            purge_end,
            inclusive="both",
        )
    ] = "purge"

    split.loc[
        development
        & dates.between(
            holdout_start,
            holdout_end,
            inclusive="both",
        )
    ] = "holdout"

    return split