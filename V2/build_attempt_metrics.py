from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd


REPOSITORIES = (
    "docker/cli",
    "prometheus/prometheus",
    "tektoncd/pipeline",
    "pytest-dev/pytest",
    "helm/helm",
    "containerd/containerd",
)


EXTERNAL_REPOSITORIES = (
    "pytest-dev/pytest",
    "helm/helm",
    "containerd/containerd",
)

ANALYSIS_START = "2025-06-23"
TRAIN_FEATURE_END = "2026-02-23"
PURGE_START = "2026-03-02"
PURGE_END = "2026-03-23"
HOLDOUT_START = "2026-03-30"
HOLDOUT_END = "2026-08-03"

PREDICTION_HORIZON_WEEKS = 4


MAD_WINDOWS = (8, 12, 26)
PRIMARY_MAD_WINDOW = 8

MAX_VALID_DURATION_MIN = 24 * 60
MIN_ATTEMPT_COVERAGE = 0.95

PR_VALIDATION_EVENTS = frozenset({
    "pull_request",
    "pull_request_target",
    "merge_group",
})

SUCCESS = {"success"}
FAILURE = {"failure", "timed_out", "startup_failure"}
EXCLUDED_CONCLUSIONS = {
    "",
    "none",
    "nan",
    "null",
    "cancelled",
    "skipped",
    "neutral",
    "stale",
    "action_required",
}


@dataclass(frozen=True)
class StudyConfig:
    repositories: tuple[str, ...] = REPOSITORIES
    external_repositories: tuple[str, ...] = EXTERNAL_REPOSITORIES
    analysis_start: str = ANALYSIS_START
    train_feature_end: str = TRAIN_FEATURE_END
    purge_start: str = PURGE_START
    purge_end: str = PURGE_END
    holdout_start: str = HOLDOUT_START
    holdout_end: str = HOLDOUT_END
    prediction_horizon_weeks: int = PREDICTION_HORIZON_WEEKS
    mad_windows: tuple[int, ...] = MAD_WINDOWS
    primary_mad_window: int = PRIMARY_MAD_WINDOW
    max_valid_duration_min: float = MAX_VALID_DURATION_MIN
    min_attempt_coverage: float = MIN_ATTEMPT_COVERAGE


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build the leakage-resistant weekly modelling panel and MAD baselines "
            "for the CI-degradation study. This script does not fit the HMM."
        )
    )
    parser.add_argument("--workflow-runs", required=True, nargs="+", type=Path)
    parser.add_argument("--prs", nargs="*", type=Path, default=[])
    parser.add_argument("--releases", nargs="*", type=Path, default=[])
    parser.add_argument("--jobs", nargs="*", type=Path, default=[])
    parser.add_argument(
        "--workflow-attempts",
        "--attempts",
        dest="workflow_attempts",
        nargs="*",
        type=Path,
        default=[],
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--allow-missing-repositories",
        action="store_true",
        help="Development only; the final study must not use this option.",
    )
    return parser.parse_args()


def validate_config(cfg: StudyConfig) -> None:
    repositories = set(cfg.repositories)
    external = set(cfg.external_repositories)
    if len(repositories) != len(cfg.repositories):
        raise ValueError("StudyConfig.repositories contains duplicates")
    if not external.issubset(repositories):
        unknown = sorted(external - repositories)
        raise ValueError(f"External repositories are not in repositories: {unknown}")
    if not repositories - external:
        raise ValueError("At least one development repository is required")
    if cfg.primary_mad_window not in cfg.mad_windows:
        raise ValueError("primary_mad_window must be included in mad_windows")
    if any(window <= 0 for window in cfg.mad_windows):
        raise ValueError("All MAD windows must be positive")
    if cfg.prediction_horizon_weeks <= 0:
        raise ValueError("prediction_horizon_weeks must be positive")
    if not 0 < cfg.min_attempt_coverage <= 1:
        raise ValueError("min_attempt_coverage must be in (0, 1]")

    dates = [
        pd.Timestamp(cfg.analysis_start),
        pd.Timestamp(cfg.train_feature_end),
        pd.Timestamp(cfg.purge_start),
        pd.Timestamp(cfg.purge_end),
        pd.Timestamp(cfg.holdout_start),
        pd.Timestamp(cfg.holdout_end),
    ]
    if dates != sorted(dates) or len(set(dates)) != len(dates):
        raise ValueError(
            "Study dates must be strictly ordered: analysis start, train end, "
            "purge start, purge end, holdout start, holdout end"
        )
    if any(timestamp.weekday() != 0 for timestamp in dates):
        raise ValueError("All study boundary dates must be Mondays")


def read_csvs(
    paths: Sequence[Path], label: str, required: bool = False
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f"{label}: file not found: {path}")
        if path.stat().st_size == 0:
            continue
        try:
            frame = pd.read_csv(path, low_memory=False)
        except pd.errors.EmptyDataError:
            continue
        frame["_source_file"] = str(path.resolve())
        frame["_source_row"] = np.arange(len(frame), dtype=np.int64)
        frames.append(frame)

    if not frames:
        if required:
            raise ValueError(f"No readable rows supplied for {label}")
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def require_columns(df: pd.DataFrame, columns: Iterable[str], label: str) -> None:
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise ValueError(f"{label} is missing columns: {', '.join(missing)}")


def utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def bool_value(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"1", "true", "yes"})


def monday(series: pd.Series) -> pd.Series:
    naive = utc(series).dt.tz_convert(None)
    return (
        naive - pd.to_timedelta(naive.dt.weekday, unit="D")
    ).dt.normalize()


def filter_to_pr_validation_runs(
    runs: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "event" not in runs.columns:
        raise ValueError("workflow runs is missing the 'event' column")
    event_norm = runs["event"].astype(str).str.strip().str.lower()
    mask = event_norm.isin(PR_VALIDATION_EVENTS)
    excluded = runs.loc[~mask].copy()
    excluded["exclusion_reason"] = "non_pr_validation_event"
    return runs.loc[mask].copy(), excluded


def canonicalize_runs(runs: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    require_columns(
        runs,
        ("repo_full", "run_id", "created_at", "status", "conclusion"),
        "workflow runs",
    )
    x = runs.copy()
    x["repo_full"] = x["repo_full"].astype(str).str.strip()
    x["run_id"] = x["run_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    x["created_at_ts"] = utc(x["created_at"])
    x["_updated_sort"] = utc(x["updated_at"]) if "updated_at" in x else pd.NaT
    x["_attempt_sort"] = (
        numeric(x["current_run_attempt"]).fillna(0)
        if "current_run_attempt" in x
        else 0
    )

    invalid_key = x["repo_full"].eq("") | x["run_id"].isin(
        {"", "nan", "None"}
    )
    invalid_date = x["created_at_ts"].isna()
    invalid = invalid_key | invalid_date
    excluded = x.loc[invalid].copy()
    excluded["exclusion_reason"] = np.where(
        invalid_key.loc[invalid], "invalid_run_key", "invalid_created_at"
    )

    x = x.loc[~invalid].copy()
    x.sort_values(
        ["repo_full", "run_id", "_attempt_sort", "_updated_sort", "_source_row"],
        inplace=True,
        na_position="first",
    )
    duplicate = x.duplicated(["repo_full", "run_id"], keep="last")
    duplicates = x.loc[duplicate].copy()
    duplicates["exclusion_reason"] = "duplicate_noncanonical_run"
    canonical = x.loc[~duplicate].copy()
    return canonical, pd.concat(
        [excluded, duplicates], ignore_index=True, sort=False
    )


def job_durations(jobs: pd.DataFrame, maximum: float) -> pd.DataFrame:
    if jobs.empty:
        return pd.DataFrame(columns=["repo_full", "run_id", "job_duration_min"])
    require_columns(
        jobs, ("repo_full", "run_id", "started_at", "completed_at"), "jobs"
    )
    x = jobs.copy()
    x["repo_full"] = x["repo_full"].astype(str).str.strip()
    x["run_id"] = x["run_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    x["started"] = utc(x["started_at"])
    x["completed"] = utc(x["completed_at"])
    x["minutes"] = (x["completed"] - x["started"]).dt.total_seconds() / 60
    x = x[x["minutes"].between(0, maximum, inclusive="both")]
    if x.empty:
        return pd.DataFrame(columns=["repo_full", "run_id", "job_duration_min"])

    out = x.groupby(["repo_full", "run_id"], as_index=False).agg(
        first_job_started=("started", "min"),
        last_job_completed=("completed", "max"),
    )
    out["job_duration_min"] = (
        out["last_job_completed"] - out["first_job_started"]
    ).dt.total_seconds() / 60
    out.loc[
        ~out["job_duration_min"].between(0, maximum, inclusive="both"),
        "job_duration_min",
    ] = np.nan
    return out[["repo_full", "run_id", "job_duration_min"]]


def attach_valid_duration(
    runs: pd.DataFrame, jobs: pd.DataFrame, maximum: float
) -> tuple[pd.DataFrame, pd.DataFrame]:
    x = runs.copy()
    durations = job_durations(jobs, maximum)
    if not durations.empty:
        x = x.merge(durations, on=["repo_full", "run_id"], how="left")
    else:
        x["job_duration_min"] = np.nan

    supplied = (
        numeric(x["ci_duration_min"])
        if "ci_duration_min" in x
        else pd.Series(np.nan, index=x.index, dtype="float64")
    )
    x["ci_duration_min_valid"] = x["job_duration_min"].fillna(supplied)
    x["duration_source"] = np.where(
        x["job_duration_min"].notna(), "jobs", "supplied_ci_duration_min"
    )
    invalid = ~x["ci_duration_min_valid"].between(
        0, maximum, inclusive="both"
    )
    invalid_rows = x.loc[
        invalid,
        ["repo_full", "run_id", "ci_duration_min_valid", "duration_source"],
    ].copy()
    invalid_rows["exclusion_reason"] = (
        "missing_negative_or_over_maximum_duration"
    )
    x.loc[invalid, "ci_duration_min_valid"] = np.nan
    return x, invalid_rows


def canonicalize_entity(
    df: pd.DataFrame, key: str, date_col: str, label: str
) -> tuple[pd.DataFrame, int]:
    if df.empty:
        return df.copy(), 0
    require_columns(df, ("repo_full", key, date_col), label)
    x = df.copy()
    x["repo_full"] = x["repo_full"].astype(str).str.strip()
    x[key] = x[key].astype(str).str.replace(r"\.0$", "", regex=True)
    x["event_ts"] = utc(x[date_col])
    x.sort_values(
        ["repo_full", key, "event_ts", "_source_row"],
        inplace=True,
        na_position="first",
    )
    duplicated = x.duplicated(["repo_full", key], keep="last")
    count = int(duplicated.sum())
    return x.loc[~duplicated & x["event_ts"].notna()].copy(), count


def weekly_runs(runs: pd.DataFrame) -> pd.DataFrame:
    return aggregate_runs(runs, ["repo_full", "week"])


def workflow_identifier(runs: pd.DataFrame) -> pd.Series:
    identifier_column = next(
        (column for column in ("workflow_id", "workflow_path", "workflow_file") if column in runs),
        None,
    )
    if identifier_column is not None:
        workflow_id = runs[identifier_column].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
        valid = ~workflow_id.isin({"", "nan", "None", "<NA>"})
    else:
        workflow_id = pd.Series("", index=runs.index, dtype="string")
        valid = pd.Series(False, index=runs.index)
    name = (
        runs["workflow_name"].astype(str).str.strip()
        if "workflow_name" in runs
        else pd.Series("unknown", index=runs.index)
    )
    name = name.mask(name.isin({"", "nan", "None", "<NA>"}), "unknown")
    return workflow_id.where(valid, "name:" + name)


def aggregate_runs(runs: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    x = runs.copy()
    x["week"] = monday(x["created_at_ts"])
    x["status_norm"] = x["status"].astype(str).str.strip().str.lower()
    x["conclusion_norm"] = (
        x["conclusion"].astype(str).str.strip().str.lower()
    )
    x["eligible_outcome"] = x["status_norm"].eq("completed") & x[
        "conclusion_norm"
    ].isin(SUCCESS | FAILURE)
    x["failure"] = x["conclusion_norm"].isin(FAILURE)
    x["head_sha_norm"] = x["head_sha"].astype(str) if "head_sha" in x else ""

    rows: list[dict] = []
    for group_key, group in x.groupby(keys, dropna=False):
        group_key = group_key if isinstance(group_key, tuple) else (group_key,)
        eligible = group[group["eligible_outcome"]]
        valid_sha = ~group["head_sha_norm"].isin({"", "nan", "None"})
        sha_counts = group.loc[valid_sha].groupby("head_sha_norm").size()
        row = dict(zip(keys, group_key))
        row.update(
            {
                "ci_runs": int(len(group)),
                "ci_outcome_runs": int(len(eligible)),
                "ci_failures": int(eligible["failure"].sum()),
                "ci_failure_rate": (
                    eligible["failure"].mean() if len(eligible) else 0.0
                ),
                "ci_duration_median_min": eligible[
                    "ci_duration_min_valid"
                ].median(),
                "ci_duration_valid_n": int(
                    eligible["ci_duration_min_valid"].notna().sum()
                ),
                "avg_runs_per_sha": (
                    sha_counts.mean() if len(sha_counts) else np.nan
                ),
                "p95_runs_per_sha": (
                    sha_counts.quantile(0.95) if len(sha_counts) else np.nan
                ),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def weekly_workflow_runs(runs: pd.DataFrame) -> pd.DataFrame:
    x = runs.copy()
    x["workflow_key"] = workflow_identifier(x)
    return aggregate_runs(x, ["repo_full", "workflow_key", "week"])


def weekly_prs(prs: pd.DataFrame) -> pd.DataFrame:
    if prs.empty:
        return pd.DataFrame(columns=["repo_full", "week"])
    x = prs.copy()
    x["created_week"] = monday(x["event_ts"])
    x["merged_ts"] = utc(x["merged_at"]) if "merged_at" in x else pd.NaT
    x["merged_week"] = monday(x["merged_ts"])
    for col in (
        "additions",
        "deletions",
        "changed_files",
        "commit_count",
        "pr_cycle_hours",
        "review_latency_hours",
        "review_count",
    ):
        x[col] = numeric(x[col]) if col in x else np.nan
    x["pr_churn"] = (
        numeric(x["pr_churn"])
        if "pr_churn" in x
        else x["additions"] + x["deletions"]
    )
    if "is_merged" in x:
        x["merged"] = bool_value(x["is_merged"])
    elif "merged_at" in x:
        x["merged"] = utc(x["merged_at"]).notna()
    else:
        x["merged"] = False

    created = x.groupby(["repo_full", "created_week"], as_index=False).agg(
        pr_count=("pr_number", "size"),
        pr_churn_median=("pr_churn", "median"),
        changed_files_median=("changed_files", "median"),
        commit_count_median=("commit_count", "median"),
        pr_cycle_hours_median=("pr_cycle_hours", "median"),
        review_latency_hours_median=("review_latency_hours", "median"),
        review_count_sum=("review_count", "sum"),
    )
    created.rename(columns={"created_week": "week"}, inplace=True)
    merged = (
        x[x["merged_ts"].notna()]
        .groupby(["repo_full", "merged_week"], as_index=False)
        .agg(merged_pr_count=("pr_number", "size"))
        .rename(columns={"merged_week": "week"})
    )
    return created.merge(merged, on=["repo_full", "week"], how="outer")


def weekly_attempts(attempts: pd.DataFrame, runs: pd.DataFrame) -> pd.DataFrame:
    if attempts.empty:
        return pd.DataFrame(columns=["repo_full", "week"])
    require_columns(attempts, ("repo_full", "run_id", "attempt_number"), "attempts")
    x = attempts.copy()
    x["repo_full"] = x["repo_full"].astype(str).str.strip()
    x["run_id"] = x["run_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    x["attempt_number"] = numeric(x["attempt_number"])
    eligible_runs = runs[["repo_full", "run_id"]].drop_duplicates()
    x = x.merge(eligible_runs, on=["repo_full", "run_id"], how="inner")
    date_col = next((c for c in ("created_at", "run_created_at", "started_at") if c in x), None)
    if date_col is None:
        raise ValueError("attempts is missing an attempt date column")
    x["week"] = monday(x[date_col])
    x = x[x["attempt_number"].ge(1) & x["week"].notna()].copy()
    x.sort_values(["repo_full", "run_id", "attempt_number", "week", "_source_row"], inplace=True)
    x.drop_duplicates(["repo_full", "run_id", "attempt_number"], keep="last", inplace=True)
    first_week = x.groupby(["repo_full", "run_id"])["week"].min().reset_index()
    max_attempt = (
        x.groupby(["repo_full", "run_id"])["attempt_number"]
        .max()
        .reset_index(name="max_attempt")
    )
    run_level = first_week.merge(max_attempt, on=["repo_full", "run_id"])

    return run_level.groupby(["repo_full", "week"], as_index=False).agg(
        rerun_count=("max_attempt", lambda s: int((s > 1).sum())),
        rerun_eligible_runs=("max_attempt", "size"),
        total_attempts=("max_attempt", "sum"),
        avg_attempts=("max_attempt", "mean"),
    )


def weekly_workflow_attempts(
    attempts: pd.DataFrame, runs: pd.DataFrame
) -> pd.DataFrame:
    if attempts.empty:
        return pd.DataFrame(columns=["repo_full", "workflow_key", "week"])
    mapping = runs[["repo_full", "run_id"]].copy()
    mapping["workflow_key"] = workflow_identifier(runs).values
    x = attempts.copy()
    x["repo_full"] = x["repo_full"].astype(str).str.strip()
    x["run_id"] = x["run_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    x = x.merge(mapping, on=["repo_full", "run_id"], how="inner")
    if x.empty:
        return pd.DataFrame(columns=["repo_full", "workflow_key", "week"])
    date_col = next((c for c in ("created_at", "run_created_at", "started_at") if c in x), None)
    if date_col is None:
        raise ValueError("attempts is missing an attempt date column")
    x["attempt_number"] = numeric(x["attempt_number"])
    x["week"] = monday(x[date_col])
    x = x[x["attempt_number"].ge(1) & x["week"].notna()].copy()
    x.sort_values(
        ["repo_full", "run_id", "attempt_number", "week", "_source_row"],
        inplace=True,
    )
    x.drop_duplicates(
        ["repo_full", "run_id", "attempt_number"], keep="last", inplace=True
    )
    run_level = x.groupby(
        ["repo_full", "workflow_key", "run_id"], as_index=False
    ).agg(week=("week", "min"), max_attempt=("attempt_number", "max"))
    return run_level.groupby(
        ["repo_full", "workflow_key", "week"], as_index=False
    ).agg(
        rerun_count=("max_attempt", lambda s: int((s > 1).sum())),
        rerun_eligible_runs=("max_attempt", "size"),
        total_attempts=("max_attempt", "sum"),
        avg_attempts=("max_attempt", "mean"),
    )


def weekly_releases(releases: pd.DataFrame) -> pd.DataFrame:
    if releases.empty:
        return pd.DataFrame(columns=["repo_full", "week", "release_count"])
    x = releases.copy()
    x["release_ts"] = (
        utc(x["published_at"]) if "published_at" in x else pd.NaT
    )
    if "created_at" in x:
        x["release_ts"] = x["release_ts"].fillna(utc(x["created_at"]))
    x["week"] = monday(x["release_ts"])
    if "draft" in x:
        x = x[~bool_value(x["draft"])]
    return (
        x[x["week"].notna()]
        .groupby(["repo_full", "week"], as_index=False)
        .agg(release_count=("release_id", "size"))
    )


def complete_grid(cfg: StudyConfig) -> pd.DataFrame:
    weeks = pd.date_range(cfg.analysis_start, cfg.holdout_end, freq="W-MON")
    return pd.MultiIndex.from_product(
        [cfg.repositories, weeks], names=["repo_full", "week"]
    ).to_frame(index=False)


def complete_workflow_grid(runs: pd.DataFrame, cfg: StudyConfig) -> pd.DataFrame:
    identities = runs[["repo_full"]].copy()
    identities["workflow_key"] = workflow_identifier(runs).values
    identities.drop_duplicates(inplace=True)
    weeks = pd.DataFrame(
        {"week": pd.date_range(cfg.analysis_start, cfg.holdout_end, freq="W-MON")}
    )
    identities["_join"] = 1
    weeks["_join"] = 1
    return identities.merge(weeks, on="_join").drop(columns="_join")


def add_temporal_features(panel: pd.DataFrame) -> pd.DataFrame:
    x = panel.sort_values(["repo_full", "week"]).copy()
    base = [
        "ci_failure_rate",
        "ci_duration_median_min",
        "ci_runs",
        "rerun_rate",
        "avg_attempts",
        "pr_count",
        "pr_churn_median",
        "review_latency_hours_median",
        "release_count",
    ]
    for col in base:
        group = x.groupby("repo_full", sort=False)[col]
        for lag in (1, 2, 4):
            x[f"{col}_lag{lag}"] = group.shift(lag)
        x[f"{col}_mean4"] = group.transform(
            lambda series: series.shift(1).rolling(4, min_periods=4).mean()
        )
        x[f"{col}_slope4"] = group.transform(
            lambda series: series.shift(1)
            .rolling(4, min_periods=4)
            .apply(
                lambda values: (
                    float(np.polyfit(np.arange(4), values, 1)[0])
                    if np.isfinite(values).all()
                    else np.nan
                ),
                raw=True,
            )
        )
    return x


def rolling_median_mad(
    values: pd.Series, window: int
) -> tuple[pd.Series, pd.Series]:
    history = values.shift(1).rolling(window, min_periods=window)
    median = history.median()
    mad = history.apply(
        lambda values: (
            float(np.median(np.abs(values - np.median(values))))
            if np.isfinite(values).all()
            else np.nan
        ),
        raw=True,
    )
    return median, mad


def add_degradation(panel: pd.DataFrame, cfg: StudyConfig) -> pd.DataFrame:
    x = panel.sort_values(["repo_full", "week"]).copy()
    holdout_start = pd.Timestamp(cfg.holdout_start)


    threshold_end = pd.Timestamp(cfg.train_feature_end)

    for window in cfg.mad_windows:
        flags = pd.Series(pd.NA, index=x.index, dtype="boolean")
        for _, indexes in x.groupby("repo_full", sort=False).groups.items():
            indexes = list(indexes)
            group = x.loc[indexes]
            components: list[pd.Series] = []

            for metric in (
                "ci_failure_rate",
                "ci_duration_median_min",
                "rerun_rate",
            ):
                median, mad = rolling_median_mad(group[metric], window)
                threshold = median + 3.0 * 1.4826 * mad

                threshold = threshold.where(mad.ne(0), median)
                development_flag = (
                    group[metric]
                    .gt(threshold)
                    .where(threshold.notna())
                    .astype("boolean")
                )

                training_history = group.loc[
                    group["week"].le(threshold_end), metric
                ].dropna().tail(window)
                if len(training_history) == window:
                    frozen_median = float(training_history.median())
                    frozen_mad = float(
                        np.median(
                            np.abs(training_history - frozen_median)
                        )
                    )
                    frozen_threshold = (
                        frozen_median + 3.0 * 1.4826 * frozen_mad
                    )
                    holdout_flag = group[metric].gt(frozen_threshold)
                    development_flag = development_flag.where(
                        group["week"].lt(holdout_start), holdout_flag
                    )
                else:
                    development_flag = development_flag.where(
                        group["week"].lt(holdout_start), pd.NA
                    )
                components.append(development_flag)

            valid = pd.concat([item.notna() for item in components], axis=1).any(axis=1)
            combined = pd.concat(
                [item.fillna(False) for item in components], axis=1
            ).any(axis=1).where(valid)
            flags.loc[indexes] = combined.astype("boolean").array

        x[f"degradation_mad{window}"] = flags

    event = x[f"degradation_mad{cfg.primary_mad_window}"]
    target = pd.Series(pd.NA, index=x.index, dtype="Int64")
    for _, indexes in x.groupby("repo_full", sort=False).groups.items():
        indexes = list(indexes)
        repository_event = event.loc[indexes]
        future = pd.concat(
            [
                repository_event.shift(-offset)
                for offset in range(1, cfg.prediction_horizon_weeks + 1)
            ],
            axis=1,
        )
        fully_known = future.notna().all(axis=1)
        value = (
            future.fillna(False)
            .any(axis=1)
            .astype("Int64")
            .where(fully_known)
        )
        target.loc[indexes] = value.array

    x[f"mad_alarm_next_{cfg.prediction_horizon_weeks}w"] = target
    return x


def assign_split(
    week: pd.Series, repo: pd.Series, cfg: StudyConfig
) -> pd.Series:
    parsed_week = pd.to_datetime(week)
    result = pd.Series("outside", index=week.index, dtype="string")
    external_mask = repo.isin(cfg.external_repositories)
    result.loc[external_mask] = "external"

    development = ~external_mask
    result.loc[
        development
        & parsed_week.between(
            pd.Timestamp(cfg.analysis_start),
            pd.Timestamp(cfg.train_feature_end),
        )
    ] = "train"
    result.loc[
        development
        & parsed_week.between(
            pd.Timestamp(cfg.purge_start), pd.Timestamp(cfg.purge_end)
        )
    ] = "purge"
    result.loc[
        development
        & parsed_week.between(
            pd.Timestamp(cfg.holdout_start), pd.Timestamp(cfg.holdout_end)
        )
    ] = "holdout"
    return result


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def quality_report(
    panel: pd.DataFrame,
    runs: pd.DataFrame,
    cfg: StudyConfig,
    run_exclusions: pd.DataFrame,
    invalid_durations: pd.DataFrame,
    pr_dupes: int,
    release_dupes: int,
    attempts: pd.DataFrame,
    workflow_panel: pd.DataFrame,
    allow_missing: bool,
) -> tuple[dict, list[str]]:
    errors: list[str] = []
    present = sorted(set(runs["repo_full"]))
    missing = sorted(set(cfg.repositories) - set(present))
    unexpected = sorted(set(present) - set(cfg.repositories))

    if missing and not allow_missing:
        errors.append("Missing repositories: " + ", ".join(missing))
    if unexpected:
        errors.append("Unexpected repositories in runs: " + ", ".join(unexpected))
    if runs.duplicated(["repo_full", "run_id"]).any():
        errors.append(
            "Canonical workflow runs still contain duplicate "
            "(repo_full, run_id)"
        )
    if (runs["ci_duration_min_valid"].dropna() < 0).any():
        errors.append("Negative validated duration remains")

    expected_rows = len(cfg.repositories) * len(
        pd.date_range(cfg.analysis_start, cfg.holdout_end, freq="W-MON")
    )
    if len(panel) != expected_rows:
        errors.append(f"Panel has {len(panel)} rows; expected {expected_rows}")
    if not panel["week"].dt.weekday.eq(0).all():
        errors.append("Not all panel weeks start on Monday")
    if panel.duplicated(["repo_full", "week"]).any():
        errors.append("Panel contains duplicate repository-week rows")
    if workflow_panel.duplicated(["repo_full", "workflow_key", "week"]).any():
        errors.append("Workflow panel contains duplicate workflow-week rows")
    stable_workflow_columns = {
        "workflow_id", "workflow_path", "workflow_file"
    }.intersection(runs.columns)
    if not stable_workflow_columns:
        errors.append(
            "Workflow runs require workflow_id, workflow_path, or workflow_file "
            "for a stable workflow-week panel"
        )

    coverage_rows: list[dict] = []
    attempts_keys = pd.DataFrame(columns=["repo_full", "run_id"])
    if attempts.empty:
        errors.append("Attempt data are required for the final analysis")
    else:
        require_columns(attempts, ("repo_full", "run_id", "attempt_number"), "attempts")
        attempts_keys = attempts[["repo_full", "run_id"]].copy()
        attempts_keys["repo_full"] = attempts_keys["repo_full"].astype(str).str.strip()
        attempts_keys["run_id"] = attempts_keys["run_id"].astype(str).str.replace(r"\.0$", "", regex=True)
        attempts_keys.drop_duplicates(inplace=True)
    for repository in cfg.repositories:
        repo_runs = runs.loc[runs["repo_full"].eq(repository), ["repo_full", "run_id"]]
        repo_attempts = attempts_keys[attempts_keys["repo_full"].eq(repository)]
        matched = repo_runs.merge(repo_attempts, on=["repo_full", "run_id"], how="inner")
        missing_run_ids = sorted(
            set(repo_runs["run_id"]) - set(repo_attempts["run_id"])
        )[:20]
        fraction = len(matched) / len(repo_runs) if len(repo_runs) else 0.0
        coverage_rows.append(
            {
                "repo_full": repository,
                "pr_validation_runs": int(len(repo_runs)),
                "runs_with_attempt_data": int(len(matched)),
                "missing_run_ids_sample": missing_run_ids,
                "fraction": float(fraction),
            }
        )
        if not allow_missing and fraction < cfg.min_attempt_coverage:
            errors.append(
                f"Attempt coverage for {repository} is {fraction:.3f}; "
                f"required minimum is {cfg.min_attempt_coverage:.3f}"
            )

    coverage_by_repo: list[dict] = []
    for repository in cfg.repositories:
        repo_runs = runs[runs["repo_full"].eq(repository)]
        weeks = monday(repo_runs["created_at_ts"]).dropna()
        minimum = weeks.min() if len(weeks) else pd.NaT
        maximum = weeks.max() if len(weeks) else pd.NaT
        coverage_by_repo.append(
            {
                "repo_full": repository,
                "first_week": None if pd.isna(minimum) else str(minimum.date()),
                "last_week": None if pd.isna(maximum) else str(maximum.date()),
            }
        )
        if not allow_missing and (pd.isna(minimum) or minimum > pd.Timestamp(cfg.analysis_start)):
            errors.append(f"Run coverage for {repository} starts after analysis_start")
        if not allow_missing and (pd.isna(maximum) or maximum < pd.Timestamp(cfg.holdout_end)):
            errors.append(f"Run coverage for {repository} ends before holdout_end")

    target_col = f"mad_alarm_next_{cfg.prediction_horizon_weeks}w"
    training_mask = panel["split"].eq("train")
    train_target_missing = int(
        panel.loc[training_mask, target_col].isna().sum()
    )

    development_repositories = set(cfg.repositories) - set(
        cfg.external_repositories
    )
    max_allowed_missing = (
        cfg.primary_mad_window + cfg.prediction_horizon_weeks
    ) * len(development_repositories)
    if train_target_missing > max_allowed_missing:
        errors.append(
            f"Training contains {train_target_missing} rows without a "
            f"{cfg.prediction_horizon_weeks}-week target "
            f"(allowed: {max_allowed_missing})"
        )

    duration_coverage = (
        runs.assign(
            duration_valid=runs["ci_duration_min_valid"].notna()
        )
        .groupby("repo_full")["duration_valid"]
        .mean()
        .to_dict()
    )

    report = {
        "status": "FAIL" if errors else "PASS",
        "errors": errors,
        "repositories_expected": list(cfg.repositories),
        "development_repositories": sorted(development_repositories),
        "external_repositories": list(cfg.external_repositories),
        "repositories_present_in_runs": present,
        "missing_repositories": missing,
        "unexpected_repositories_ignored_by_grid": unexpected,
        "canonical_run_rows": int(len(runs)),
        "unique_run_ids_by_repository": int(
            runs[["repo_full", "run_id"]].drop_duplicates().shape[0]
        ),
        "excluded_or_duplicate_run_rows": int(len(run_exclusions)),
        "invalid_duration_rows": int(len(invalid_durations)),
        "valid_duration_fraction_by_repository": {
            str(repo): float(value)
            for repo, value in duration_coverage.items()
        },
        "duplicate_pr_rows_removed": pr_dupes,
        "duplicate_release_rows_removed": release_dupes,
        "attempt_coverage_by_repository": coverage_rows,
        "run_week_coverage_by_repository": coverage_by_repo,
        "panel_rows": int(len(panel)),
        "workflow_panel_rows": int(len(workflow_panel)),
        "zero_run_repository_weeks": int(panel["ci_runs"].eq(0).sum()),
        "rows_by_split": {
            str(key): int(value)
            for key, value in panel["split"].value_counts().items()
        },
        "training_target_missing": train_target_missing,
        "training_target_missing_allowed": max_allowed_missing,
        "target_prevalence_by_split": {
            str(key): (None if pd.isna(value) else float(value))
            for key, value in panel.groupby("split")[target_col].mean().items()
        },
        "date_min": str(panel["week"].min().date()),
        "date_max": str(panel["week"].max().date()),
    }
    return report, errors


def main() -> int:
    args = parse_args()
    cfg = StudyConfig()
    validate_config(cfg)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    raw_runs = read_csvs(args.workflow_runs, "workflow runs", required=True)
    raw_prs = read_csvs(args.prs, "pull requests")
    raw_releases = read_csvs(args.releases, "releases")
    raw_jobs = read_csvs(args.jobs, "jobs")
    raw_attempts = read_csvs(args.workflow_attempts, "attempts")

    runs, canonical_exclusions = canonicalize_runs(raw_runs)
    runs, event_exclusions = filter_to_pr_validation_runs(runs)
    run_exclusions = pd.concat(
        [canonical_exclusions, event_exclusions],
        ignore_index=True,
        sort=False,
    )
    runs, invalid_durations = attach_valid_duration(
        runs,
        raw_jobs,
        cfg.max_valid_duration_min,
    )
    prs, pr_dupes = canonicalize_entity(
        raw_prs, "pr_number", "created_at", "pull requests"
    )
    releases, release_dupes = canonicalize_entity(
        raw_releases,
        "release_id",
        (
            "published_at"
            if "published_at" in raw_releases
            else "created_at"
        ),
        "releases",
    )

    panel = complete_grid(cfg)
    weekly_tables = (
        weekly_runs(runs),
        weekly_prs(prs),
        weekly_releases(releases),
        weekly_attempts(raw_attempts, runs),
    )
    for weekly in weekly_tables:
        panel = panel.merge(weekly, on=["repo_full", "week"], how="left")

    if "rerun_count" in panel.columns:
        panel["rerun_rate"] = panel["rerun_count"] / panel["rerun_eligible_runs"].replace(0, np.nan)
    else:
        panel["rerun_rate"] = np.nan
        panel["avg_attempts"] = np.nan

    count_cols = (
        "ci_runs",
        "ci_outcome_runs",
        "ci_failures",
        "ci_duration_valid_n",
        "pr_count",
        "merged_pr_count",
        "review_count_sum",
        "release_count",
        "rerun_count",
        "rerun_eligible_runs",
        "total_attempts",
    )
    for col in count_cols:
        if col not in panel:
            panel[col] = 0
        panel[col] = panel[col].fillna(0).astype("int64")

    panel = add_temporal_features(panel)
    panel = add_degradation(panel, cfg)
    panel["split"] = assign_split(
        panel["week"], panel["repo_full"], cfg
    )
    panel["week"] = pd.to_datetime(panel["week"])

    workflow_panel = complete_workflow_grid(runs, cfg)
    workflow_tables = (
        weekly_workflow_runs(runs),
        weekly_workflow_attempts(raw_attempts, runs),
    )
    for weekly in workflow_tables:
        workflow_panel = workflow_panel.merge(
            weekly, on=["repo_full", "workflow_key", "week"], how="left"
        )
    workflow_count_cols = (
        "ci_runs",
        "ci_outcome_runs",
        "ci_failures",
        "ci_duration_valid_n",
        "rerun_count",
        "rerun_eligible_runs",
        "total_attempts",
    )
    for col in workflow_count_cols:
        if col not in workflow_panel:
            workflow_panel[col] = 0
        workflow_panel[col] = workflow_panel[col].fillna(0).astype("int64")
    workflow_panel["rerun_rate"] = workflow_panel["rerun_count"] / workflow_panel[
        "rerun_eligible_runs"
    ].replace(0, np.nan)
    workflow_panel["split"] = assign_split(
        workflow_panel["week"], workflow_panel["repo_full"], cfg
    )
    workflow_panel["week"] = pd.to_datetime(workflow_panel["week"])

    report, errors = quality_report(
        panel,
        runs,
        cfg,
        run_exclusions,
        invalid_durations,
        pr_dupes,
        release_dupes,
        raw_attempts,
        workflow_panel,
        args.allow_missing_repositories,
    )

    panel_out = args.output_dir / "repository_week_modelling_panel.csv"
    workflow_panel_out = args.output_dir / "workflow_week_modelling_panel.csv"
    exclusions_out = args.output_dir / "excluded_rows.csv"
    report_out = args.output_dir / "data_quality_report.json"
    metadata_out = args.output_dir / "run_metadata.json"

    panel.to_csv(panel_out, index=False, date_format="%Y-%m-%d")
    workflow_panel.to_csv(workflow_panel_out, index=False, date_format="%Y-%m-%d")
    pd.concat(
        [run_exclusions, invalid_durations],
        ignore_index=True,
        sort=False,
    ).to_csv(exclusions_out, index=False)
    report_out.write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    inputs = [
        *args.workflow_runs,
        *args.prs,
        *args.releases,
        *args.jobs,
        *args.workflow_attempts,
    ]
    target_col = f"mad_alarm_next_{cfg.prediction_horizon_weeks}w"
    metadata = {
        "script_scope": (
            "Weekly panel and MAD-baseline construction; no HMM is fitted"
        ),
        "study_config": asdict(cfg),
        "failure_conclusions": sorted(FAILURE),
        "success_conclusions": sorted(SUCCESS),
        "excluded_conclusions": sorted(EXCLUDED_CONCLUSIONS),
        "duration_policy": (
            "Jobs wall-clock span preferred; otherwise validated supplied "
            "ci_duration_min; updated_at is never used as a duration endpoint"
        ),
        "mad_definition": (
            "median(previous W) + 3 * 1.4826 * MAD(previous W); "
            "full W required"
        ),
        "holdout_threshold_policy": (
            "Frozen from the final W non-missing observations ending no later "
            "than train_feature_end; purge observations are excluded"
        ),
        "alarm_column": target_col,
        "alarm_definition": (
            f"1 if MAD baseline fires in any of t+1..t+"
            f"{cfg.prediction_horizon_weeks}; baseline signal only, not ground truth"
        ),
        "input_sha256": {
            str(path.resolve()): file_sha256(path)
            for path in inputs
            if path.exists()
        },
        "outputs": {
            str(path.name): file_sha256(path)
            for path in (panel_out, workflow_panel_out, exclusions_out, report_out)
        },
    }
    metadata_out.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print(json.dumps(report, indent=2, ensure_ascii=False))
    if errors:
        print(
            "QUALITY GATE FAILED. Model fitting must not start.",
            file=sys.stderr,
        )
        return 2
    print(f"QUALITY GATE PASSED. Panel written to {panel_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())