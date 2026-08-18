from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ci_common import CORE_METRICS, load_config, output_dir, sqlite_path, write_json


REVIEW_METRICS = (
    "feedback_latency_median_min",
    "feedback_latency_p90_min",
    "queue_latency_median_min",
    "execution_span_median_min",
    "failure_rate",
    "rerun_rate",
    "workflow_adjusted_latency_log",
    "latency_log",
    "pr_cycle_hours_median",
    "qualified_review_latency_hours_median",
    "merged_pr_count",
)


def finite_number(value: Any) -> float | int | None:
    if value is None or pd.isna(value):
        return None
    number = float(value)
    return int(number) if number.is_integer() else number


def describe(values: pd.Series, prefix: str, include_p90: bool = False) -> dict[str, Any]:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    result = {
        f"{prefix}_median": finite_number(numeric.median()),
        f"{prefix}_iqr": finite_number(numeric.quantile(0.75) - numeric.quantile(0.25)),
        f"{prefix}_minimum": finite_number(numeric.min()),
        f"{prefix}_maximum": finite_number(numeric.max()),
    }
    if include_p90:
        result[f"{prefix}_p90"] = finite_number(numeric.quantile(0.90))
    return result


def bool_series(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False)
    return values.fillna("").astype(str).str.lower().eq("true")


def monday(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce", utc=True).dt.tz_convert(None)
    return (parsed - pd.to_timedelta(parsed.dt.weekday, unit="D")).dt.normalize()


def reconstruction(panel: pd.DataFrame, numerator: str, denominator: str, rate: str) -> dict[str, Any]:
    expected = panel[numerator] / panel[denominator].replace(0, np.nan)
    actual = pd.to_numeric(panel[rate], errors="coerce")
    equal = np.isclose(expected, actual, rtol=0, atol=1e-15, equal_nan=True)
    differences = (expected - actual).abs()
    return {
        "exact_within_absolute_tolerance_1e-15": bool(equal.all()),
        "mismatch_rows": int((~equal).sum()),
        "maximum_absolute_difference": finite_number(differences.max()),
        "numerator_column": numerator,
        "denominator_column": denominator,
        "rate_column": rate,
    }


def mad_evaluability(panel: pd.DataFrame, windows: list[int]) -> list[dict[str, Any]]:
    ordered = panel.sort_values(["repo_full", "week"]).copy()
    flags: dict[tuple[int, str], pd.Series] = {}
    for window in windows:
        for metric in CORE_METRICS:
            flag = pd.Series(False, index=ordered.index)
            for _, indexes in ordered.groupby("repo_full", sort=False).groups.items():
                values = pd.to_numeric(ordered.loc[indexes, metric], errors="coerce")
                history = values.shift(1).rolling(window, min_periods=window)
                center = history.median()
                mad = history.apply(
                    lambda item: float(np.median(np.abs(item - np.median(item)))),
                    raw=True,
                )
                flag.loc[indexes] = values.notna() & center.notna() & mad.gt(0)
            flags[(window, metric)] = flag

    rows = []
    for (repo, split), indexes in ordered.groupby(["repo_full", "split"], sort=True).groups.items():
        for window in windows:
            component = {metric: flags[(window, metric)].loc[indexes] for metric in CORE_METRICS}
            frame = pd.DataFrame(component)
            rows.append(
                {
                    "repo_full": repo,
                    "split": split,
                    "window_weeks": window,
                    "weeks": len(indexes),
                    **{f"{metric}_evaluable": int(value.sum()) for metric, value in component.items()},
                    "any_core_metric_evaluable": int(frame.any(axis=1).sum()),
                    "all_core_metrics_evaluable": int(frame.all(axis=1).sum()),
                    "definition": "current value present, W prior values present, and prior-window MAD greater than zero",
                }
            )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Review measurement outputs without fitting models.")
    parser.add_argument("--config", default="analysis_config.yaml")
    args = parser.parse_args()
    cfg = load_config(args.config)
    out = output_dir(cfg, args.config)

    panel = pd.read_csv(out / "repository_week_panel_audited.csv", parse_dates=["week"])
    workflow = pd.read_csv(out / "workflow_week_panel.csv", parse_dates=["week"])
    attempts = pd.read_csv(out / "attempt_measurements.csv.gz", low_memory=False)
    attempts["week"] = monday(attempts["week"])
    attempts["eligible_outcome"] = bool_series(attempts["eligible_outcome"])
    attempts["valid_feedback"] = bool_series(attempts["valid_feedback"])

    panel_keys = panel[["repo_full", "week", "split"]]
    analysis_attempts = attempts.merge(panel_keys, on=["repo_full", "week"], how="inner", validate="many_to_one")
    eligible_latency = analysis_attempts[
        analysis_attempts["eligible_outcome"] & analysis_attempts["valid_feedback"]
    ]

    review_rows = []
    count_columns = (
        "attempts_total",
        "logical_run_n",
        "outcome_n",
        "latency_n",
        "failure_count",
        "rerun_count",
        "failed_then_passed_count",
    )
    for (repo, split), group in panel.groupby(["repo_full", "split"], sort=True):
        latency = eligible_latency[
            eligible_latency["repo_full"].eq(repo) & eligible_latency["split"].eq(split)
        ]
        row: dict[str, Any] = {
            "repo_full": repo,
            "split": split,
            "weeks": len(group),
            **{column: int(group[column].sum()) for column in count_columns},
            **describe(latency["feedback_latency_min"], "feedback_latency_min", include_p90=True),
            "queue_latency_min_median": finite_number(latency["queue_latency_min"].median()),
            "execution_span_min_median": finite_number(latency["execution_span_min"].median()),
            **describe(group["failure_rate"], "failure_rate"),
            **describe(group["rerun_rate"], "rerun_rate"),
            **describe(group["workflow_adjusted_latency_log"], "workflow_adjusted_latency_log"),
        }
        for metric in CORE_METRICS:
            missing = int(group[metric].isna().sum())
            row[f"{metric}_missing_weeks"] = missing
            row[f"{metric}_missing_percent"] = 100 * missing / len(group)
        outcome_low = group["outcome_n"].lt(cfg["measurement"]["minimum_weekly_outcomes"])
        latency_low = group["latency_n"].lt(cfg["measurement"]["minimum_weekly_duration_observations"])
        any_low = outcome_low | latency_low
        row.update(
            low_outcome_support_weeks=int(outcome_low.sum()),
            low_outcome_support_percent=100 * outcome_low.mean(),
            low_latency_support_weeks=int(latency_low.sum()),
            low_latency_support_percent=100 * latency_low.mean(),
            any_low_support_weeks=int(any_low.sum()),
            any_low_support_percent=100 * any_low.mean(),
        )
        review_rows.append(row)
    pd.DataFrame(review_rows).to_csv(out / "metric_review_by_repository.csv", index=False)

    created = pd.to_datetime(attempts["created_at"], errors="coerce", utc=True)
    first_job = pd.to_datetime(attempts["first_job_started_at"], errors="coerce", utc=True)
    last_job = pd.to_datetime(attempts["last_job_completed_at"], errors="coerce", utc=True)
    raw_feedback = (last_job - created).dt.total_seconds() / 60
    raw_queue = (first_job - created).dt.total_seconds() / 60
    raw_execution = (last_job - first_job).dt.total_seconds() / 60
    maximum = float(cfg["measurement"]["maximum_feedback_minutes"])

    variance_findings = []
    for (repo, split), group in panel.groupby(["repo_full", "split"], sort=True):
        for metric in REVIEW_METRICS:
            values = pd.to_numeric(group[metric], errors="coerce").dropna()
            if values.empty or values.eq(0).all() or values.nunique() <= 1:
                variance_findings.append(
                    {
                        "repo_full": repo,
                        "split": split,
                        "metric": metric,
                        "nonmissing": len(values),
                        "all_missing": values.empty,
                        "all_zero": bool(len(values) and values.eq(0).all()),
                        "zero_variance": bool(len(values) and values.nunique() <= 1),
                    }
                )

    correlations = []
    correlation_groups = [("GLOBAL", "all", panel)] + [
        (repo, split, group)
        for (repo, split), group in panel.groupby(["repo_full", "split"], sort=True)
    ]
    for repo, split, group in correlation_groups:
        matrix = group[list(CORE_METRICS)].corr(min_periods=2)
        correlations.append(
            {
                "repo_full": repo,
                "split": split,
                "weeks": len(group),
                "pairs": {
                    "latency_log__failure_rate": finite_number(matrix.loc["latency_log", "failure_rate"]),
                    "latency_log__rerun_rate": finite_number(matrix.loc["latency_log", "rerun_rate"]),
                    "failure_rate__rerun_rate": finite_number(matrix.loc["failure_rate", "rerun_rate"]),
                },
            }
        )

    pr_coverage = []
    for (repo, split), group in panel.groupby(["repo_full", "split"], sort=True):
        pr_coverage.append(
            {
                "repo_full": repo,
                "split": split,
                "weeks": len(group),
                "cycle_time_nonmissing_weeks": int(group["pr_cycle_hours_median"].notna().sum()),
                "cycle_time_coverage": float(group["pr_cycle_hours_median"].notna().mean()),
                "review_latency_nonmissing_weeks": int(group["qualified_review_latency_hours_median"].notna().sum()),
                "review_latency_coverage": float(group["qualified_review_latency_hours_median"].notna().mean()),
                "weeks_with_created_prs": int(group["created_pr_count"].gt(0).sum()),
                "weeks_with_merged_prs": int(group["merged_pr_count"].gt(0).sum()),
            }
        )

    repositories = cfg["study"]["development_repositories"] + cfg["study"]["external_repositories"]
    placeholders = ",".join("?" for _ in repositories)
    with sqlite3.connect(sqlite_path(cfg, args.config)) as con:
        reviews = pd.read_sql_query(
            f"SELECT repo_full, user_login, user_type, state FROM pr_reviews WHERE repo_full IN ({placeholders})",
            con,
            params=repositories,
        )
    bot = reviews["user_type"].fillna("").str.lower().eq("bot") | reviews["user_login"].fillna("").str.lower().str.contains(r"\[bot\]$|bot$", regex=True)
    qualified = reviews["state"].fillna("").str.upper().isin(cfg["measurement"]["qualified_review_states"])
    reviews = reviews.assign(is_bot=bot, is_qualified=qualified)
    bot_review_counts = []
    for repo, group in reviews.groupby("repo_full", sort=True):
        bot_review_counts.append(
            {
                "repo_full": repo,
                "reviews_total": len(group),
                "bot_reviews_total": int(group["is_bot"].sum()),
                "qualified_reviews_total": int(group["is_qualified"].sum()),
                "qualified_bot_reviews_excluded": int((group["is_bot"] & group["is_qualified"]).sum()),
                "qualified_human_reviews_retained": int((~group["is_bot"] & group["is_qualified"]).sum()),
            }
        )

    workflow_spans = workflow.groupby(["repo_full", "workflow_key"])["week"].agg(["min", "max", "nunique"])
    expected_within_span = ((workflow_spans["max"] - workflow_spans["min"]).dt.days // 7 + 1)
    workflow_count = int(workflow.groupby("repo_full")["workflow_key"].nunique().sum())
    complete_grid_rows = workflow_count * len(panel["week"].unique())

    warning_groups = []
    for key, group in attempts.groupby(["repo_full", "workflow_key", "week"], dropna=False):
        eligible = group[group["eligible_outcome"]]
        if eligible.empty:
            continue
        queue_missing = eligible["queue_latency_min"].notna().sum() == 0
        execution_missing = eligible["execution_span_min"].notna().sum() == 0
        if queue_missing or execution_missing:
            warning_groups.append(
                {
                    "repo_full": key[0],
                    "workflow_key": str(key[1]),
                    "week": str(key[2]),
                    "eligible_attempts": len(eligible),
                    "queue_warning_line_97": bool(queue_missing),
                    "execution_warning_line_98": bool(execution_missing),
                }
            )

    report = {
        "review_scope": "Read-only review of step 01 and 02 outputs; no detector or model fitted",
        "failure_rate_reconstruction": reconstruction(panel, "failure_count", "outcome_n", "failure_rate"),
        "rerun_rate_reconstruction": reconstruction(panel, "rerun_count", "logical_run_n", "rerun_rate"),
        "durations": {
            "attempt_rows": len(attempts),
            "analysis_window_attempt_rows": len(analysis_attempts),
            "attempts_with_missing_job_group": int(attempts["job_count"].isna().sum()),
            "attempts_without_any_completed_job": int(attempts["completed_job_count"].fillna(0).eq(0).sum()),
            "attempts_without_valid_feedback_latency": int((~attempts["valid_feedback"]).sum()),
            "missing_created_at": int(created.isna().sum()),
            "missing_latest_job_completion": int(last_job.isna().sum()),
            "negative_feedback_duration": int(raw_feedback.lt(0).sum()),
            "feedback_duration_over_maximum": int(raw_feedback.gt(maximum).sum()),
            "negative_queue_duration": int(raw_queue.lt(0).sum()),
            "negative_execution_span": int(raw_execution.lt(0).sum()),
            "decomposition_mismatch_over_1e-9_minutes": int((raw_feedback.sub(raw_queue + raw_execution).abs() > 1e-9).sum()),
            "valid_output_negative_feedback": int(pd.to_numeric(attempts["feedback_latency_min"], errors="coerce").lt(0).sum()),
            "valid_output_feedback_over_maximum": int(pd.to_numeric(attempts["feedback_latency_min"], errors="coerce").gt(maximum).sum()),
        },
        "support_and_missingness": {
            "repository_weeks": len(panel),
            "weeks_with_outcome_n_below_5": int(panel["outcome_n"].lt(5).sum()),
            "weeks_with_latency_n_below_5": int(panel["latency_n"].lt(5).sum()),
            "weeks_with_missing_workflow_adjusted_latency": int(panel["workflow_adjusted_latency_log"].isna().sum()),
            "core_metric_missing_weeks": {metric: int(panel[metric].isna().sum()) for metric in CORE_METRICS},
        },
        "duplicate_keys": {
            "repository_week": int(panel.duplicated(["repo_full", "week"]).sum()),
            "workflow_week": int(workflow.duplicated(["repo_full", "workflow_key", "week"]).sum()),
            "attempt": int(attempts.duplicated(["repo_full", "run_id", "attempt_number"]).sum()),
        },
        "all_zero_or_zero_variance_metrics_by_repository_split": variance_findings,
        "core_metric_correlations": correlations,
        "pr_outcome_coverage": pr_coverage,
        "bot_review_exclusions": bot_review_counts,
        "mad_evaluable_observations": mad_evaluability(
            panel,
            [cfg["mad"]["primary_window_weeks"], *cfg["mad"]["sensitivity_windows_weeks"]],
        ),
        "workflow_panel_semantics": {
            "actual_rows": len(workflow),
            "distinct_workflows_across_repositories": workflow_count,
            "analysis_weeks": int(panel["week"].nunique()),
            "complete_workflow_by_analysis_week_grid_rows": complete_grid_rows,
            "previous_panel_rows": 3363,
            "observed_only": True,
            "missing_workflow_weeks_inside_first_to_last_observed_span": int((expected_within_span - workflow_spans["nunique"]).sum()),
            "weeks_outside_repository_panel_period": int((~workflow["week"].isin(panel["week"].unique())).sum()),
            "consequence": "Absent workflow-week rows conflate workflow inactivity, entry/exit, and missing collection; concentration uses observed attempts only and cannot diagnose absent workflows without an explicit grid.",
        },
        "definition_verification": {
            "feedback_latency": "Verified in 01_build_measurement_panels.py:52 as latest job completed_at minus attempt created_at.",
            "updated_at_endpoint": "Verified absent from duration calculations; updated_at is not selected into the attempt measurement query.",
            "logical_run_week": "Verified from workflow_runs.created_at (logical_run_created_at) at lines 32 and 51, not attempt creation time.",
            "numerators_denominators": "failure_count/outcome_n and rerun_count/logical_run_n are retained and reconstruct their rates.",
            "external_isolation": "UNRESOLVED PRE-MODELLING ISSUE: workflow baselines are repository-specific and date-limited, but ci_common.robust_training_standardize() estimates each repository from split=train rows. External repositories contain only split=external rows, so their medians and scales are NaN. This may produce NaN-standardized external failure and rerun metrics and invalidate external HMM evaluation. No model was run and this issue was not fixed.",
            "holdout_isolation": "Workflow baselines use dates through train_end only; robust standardization uses split=train only. No fitted transformation uses holdout rows in steps 01 or 02.",
            "logical_runs_assigned_to_multiple_weeks": int(attempts.groupby(["repo_full", "run_id"])["week"].nunique().gt(1).sum()),
            "logical_run_week_mismatches": int((attempts["week"] != monday(attempts["logical_run_created_at"])).sum()),
        },
        "resolved_warnings": [
            {
                "warning": "Mean of empty slice",
                "historical_count": 8,
                "source": "01_build_measurement_panels.py aggregate_ci: formerly the unguarded queue and execution-span medians",
                "affected_groups": warning_groups,
                "reason": "Each affected observed workflow-week has eligible outcomes but no valid job-derived duration for any eligible attempt. Pandas median delegates an all-NaN array to NumPy nanmedian.",
                "resolution": "The medians now drop missing values and explicitly return NaN when no valid values remain; no values are replaced with zero and no warnings are suppressed.",
                "impact": "The four workflow-weeks retain missing latency diagnostics and contribute no workflow-adjusted latency residual. Repository-week core metrics remain unchanged because other workflows provide valid observations.",
            }
        ],
        "warnings": [
            {
                "warning": "Release context is unavailable; candidate episodes must be triangulated against releases separately",
                "count": 1,
                "source": "02_validate_measurements.py",
                "reason": "No releases table or configured releases.csv was available.",
                "impact": "Release context remains missing rather than being interpreted as zero; core CI and PR metrics are unaffected.",
            },
        ],
    }
    write_json(out / "metric_review_global.json", report)
    print(json.dumps({"csv_rows": len(review_rows), "global_report": "metric_review_global.json"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())