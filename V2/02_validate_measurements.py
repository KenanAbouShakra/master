from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from ci_common import load_config, output_dir, repositories, write_json


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="analysis_config.yaml")
    args = parser.parse_args()
    cfg = load_config(args.config)
    out = output_dir(cfg, args.config)
    repo = pd.read_csv(out / "repository_week_panel.csv", parse_dates=["week"])
    workflow = pd.read_csv(out / "workflow_week_panel.csv", parse_dates=["week"])
    metadata = json.loads((out / "measurement_metadata.json").read_text())
    errors, warnings = [], []
    expected_repos = repositories(cfg)
    if set(repo["repo_full"]) != set(expected_repos):
        errors.append("Repository population does not equal the frozen six repositories")
    if repo.duplicated(["repo_full", "week"]).any():
        errors.append("Duplicate repository-week keys")
    if workflow.duplicated(["repo_full", "workflow_key", "week"]).any():
        errors.append("Duplicate workflow-week keys")
    if not repo["week"].dt.weekday.eq(0).all():
        errors.append("Weeks are not Monday anchored")
    calibration_columns = {
        "external_calibration",
        "external_evaluation_eligible",
    }
    if not calibration_columns.issubset(repo.columns):
        errors.append("External calibration flags are missing")
    else:
        external_repositories = set(cfg["study"]["external_repositories"])
        calibration_weeks = int(cfg["study"]["external_calibration_weeks"])
        for name in expected_repos:
            group = repo[repo["repo_full"].eq(name)].sort_values("week")
            calibration = group["external_calibration"].fillna(False).astype(bool)
            evaluation = group["external_evaluation_eligible"].fillna(False).astype(bool)
            if name in external_repositories:
                observed = group[group["attempts_total"].gt(0)]
                expected = set(observed["week"].iloc[:calibration_weeks])
                actual = set(group.loc[calibration, "week"])
                if actual != expected or len(actual) != calibration_weeks:
                    errors.append(
                        f"{name} does not use its first {calibration_weeks} "
                        "observed weeks for external calibration"
                    )
                expected_evaluation = set(observed["week"].iloc[calibration_weeks:])
                actual_evaluation = set(group.loc[evaluation, "week"])
                if actual_evaluation != expected_evaluation:
                    errors.append(
                        f"{name} external evaluation eligibility is incorrect"
                    )
                if (calibration & evaluation).any():
                    errors.append(
                        f"{name} has overlapping calibration and evaluation weeks"
                    )
            elif calibration.any() or evaluation.any():
                errors.append(
                    f"Development repository {name} has external calibration flags"
                )
    reconstructed_failure = repo["failure_count"] / repo["outcome_n"].replace(0, np.nan)
    reconstructed_rerun = repo["rerun_count"] / repo["logical_run_n"].replace(0, np.nan)
    if not np.allclose(reconstructed_failure, repo["failure_rate"], equal_nan=True):
        errors.append("Failure counts/denominators do not reconstruct failure_rate")
    if not np.allclose(reconstructed_rerun, repo["rerun_rate"], equal_nan=True):
        errors.append("Rerun counts/denominators do not reconstruct rerun_rate")
    if (repo["failure_count"] > repo["outcome_n"]).any():
        errors.append("failure_count exceeds outcome_n")
    if (repo["rerun_count"] > repo["logical_run_n"]).any():
        errors.append("rerun_count exceeds logical_run_n")
    if (repo["feedback_latency_median_min"].dropna() < 0).any():
        errors.append("Negative feedback latency remains")
    required_attempt_coverage = float(cfg["measurement"]["minimum_attempt_coverage"])
    for item in metadata.get("attempt_coverage_by_repository", []):
        if float(item.get("attempt_coverage", 0)) < required_attempt_coverage:
            errors.append(f"Attempt coverage for {item['repo_full']} is {item.get('attempt_coverage', 0):.3f}; required {required_attempt_coverage:.3f}")
    minimum_n = int(cfg["measurement"]["minimum_weekly_outcomes"])
    low_support = repo["outcome_n"].lt(minimum_n)
    repo["low_outcome_support"] = low_support
    repo["low_latency_support"] = repo["latency_n"].lt(int(cfg["measurement"]["minimum_weekly_duration_observations"]))
    repo["missing_core_metric_count"] = repo[["latency_log", "failure_rate", "rerun_rate"]].isna().sum(axis=1)
    if low_support.mean() > .20:
        warnings.append(f"{low_support.mean():.1%} of repository-weeks have fewer than {minimum_n} eligible outcomes")
    if "release_count" not in repo or repo["release_count"].isna().all():
        warnings.append("Release context is unavailable; candidate episodes must be triangulated against releases separately")
    coverage = []
    for name, group in repo.groupby("repo_full"):
        coverage.append({
            "repo_full": name,
            "first_week": str(group["week"].min().date()),
            "last_week": str(group["week"].max().date()),
            "weeks": len(group),
            "zero_attempt_weeks": int(group["attempts_total"].eq(0).sum()),
            "outcome_support_median": float(group["outcome_n"].median()),
            "latency_coverage": float(group["feedback_latency_median_min"].notna().mean()),
            "failure_coverage": float(group["failure_rate"].notna().mean()),
            "rerun_coverage": float(group["rerun_rate"].notna().mean()),
            "pr_cycle_coverage": float(group["pr_cycle_hours_median"].notna().mean()),
            "review_coverage": float(group["qualified_review_latency_hours_median"].notna().mean()),
        })
    report = {
        "status": "PASS" if not errors else "FAIL",
        "errors": errors,
        "warnings": warnings,
        "coverage_by_repository": coverage,
        "rows_by_split": repo["split"].value_counts(dropna=False).to_dict(),
        "workflow_count_by_repository": workflow.groupby("repo_full")["workflow_key"].nunique().to_dict(),
        "attempt_coverage_by_repository": metadata.get("attempt_coverage_by_repository", []),
        "external_calibration_by_repository": {
            name: {
                "calibration_weeks": int(
                    repo.loc[
                        repo["repo_full"].eq(name)
                        & repo.get("external_calibration", False),
                        "week",
                    ].nunique()
                ),
                "evaluation_weeks": int(
                    repo.loc[
                        repo["repo_full"].eq(name)
                        & repo.get("external_evaluation_eligible", False),
                        "week",
                    ].nunique()
                ),
            }
            for name in cfg["study"]["external_repositories"]
        } if calibration_columns.issubset(repo.columns) else {},
    }
    repo.to_csv(out / "repository_week_panel_audited.csv", index=False, date_format="%Y-%m-%d")
    write_json(out / "data_quality_report.json", report)
    print(json.dumps(report, indent=2))
    if errors:
        print("QUALITY GATE FAILED: do not fit models")
        return 2
    print("QUALITY GATE PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())