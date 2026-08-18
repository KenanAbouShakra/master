from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from ci_common import add_external_calibration_flags, assign_split, load_config, monday, output_dir, repositories, sha256, sqlite_path, write_json


def read_sql(con: sqlite3.Connection, query: str, params=()) -> pd.DataFrame:
    return pd.read_sql_query(query, con, params=params)


def check_tables(con: sqlite3.Connection) -> None:
    required = {"workflow_runs", "run_attempts", "workflow_jobs", "pull_requests", "pr_reviews", "pr_ci_links", "workflows"}
    found = {row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    missing = sorted(required - found)
    if missing:
        raise RuntimeError("SQLite database lacks required tables: " + ", ".join(missing))


def build_attempt_table(con: sqlite3.Connection, cfg: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    attempts = read_sql(con, """
        SELECT a.repo_full, CAST(a.run_id AS TEXT) AS run_id,
               a.attempt_number, a.workflow_id, a.name AS workflow_name,
               a.path AS workflow_path, a.event, a.status, a.conclusion,
               a.created_at, a.run_started_at, a.head_sha,
               r.created_at AS logical_run_created_at,
               r.current_run_attempt
        FROM run_attempts a
        JOIN workflow_runs r ON r.repo_full=a.repo_full AND r.run_id=a.run_id
    """)
    jobs = read_sql(con, """
        SELECT repo_full, CAST(run_id AS TEXT) AS run_id, attempt_number,
               MIN(started_at) AS first_job_started_at,
               MAX(completed_at) AS last_job_completed_at,
               COUNT(*) AS job_count,
               SUM(CASE WHEN completed_at IS NOT NULL THEN 1 ELSE 0 END) AS completed_job_count
        FROM workflow_jobs
        GROUP BY repo_full, run_id, attempt_number
    """)
    x = attempts.merge(jobs, on=["repo_full", "run_id", "attempt_number"], how="left")
    events = set(cfg["measurement"]["pr_validation_events"])
    x = x[x["repo_full"].isin(repositories(cfg)) & x["event"].isin(events)].copy()
    for col in ("created_at", "logical_run_created_at", "run_started_at", "first_job_started_at", "last_job_completed_at"):
        x[col + "_ts"] = pd.to_datetime(x[col], errors="coerce", utc=True)
    x["week"] = monday(x["logical_run_created_at"])
    x["feedback_latency_min"] = (x["last_job_completed_at_ts"] - x["created_at_ts"]).dt.total_seconds() / 60
    x["queue_latency_min"] = (x["first_job_started_at_ts"] - x["created_at_ts"]).dt.total_seconds() / 60
    x["execution_span_min"] = (x["last_job_completed_at_ts"] - x["first_job_started_at_ts"]).dt.total_seconds() / 60
    maximum = float(cfg["measurement"]["maximum_feedback_minutes"])
    x["valid_feedback"] = x["feedback_latency_min"].between(0, maximum, inclusive="both")
    x.loc[~x["valid_feedback"], ["feedback_latency_min", "queue_latency_min", "execution_span_min"]] = np.nan
    success = set(cfg["measurement"]["success_conclusions"])
    failure = set(cfg["measurement"]["failure_conclusions"])
    x["conclusion_norm"] = x["conclusion"].fillna("").astype(str).str.lower()
    x["eligible_outcome"] = x["conclusion_norm"].isin(success | failure)
    x["failure"] = x["conclusion_norm"].isin(failure)
    x["workflow_key"] = x["workflow_id"].astype("Int64").astype(str)
    missing_id = x["workflow_id"].isna()
    x.loc[missing_id, "workflow_key"] = "path:" + x.loc[missing_id, "workflow_path"].fillna(x.loc[missing_id, "workflow_name"]).fillna("unknown")
    exclusions = x.loc[~x["eligible_outcome"] | ~x["valid_feedback"], [
        "repo_full", "run_id", "attempt_number", "conclusion", "feedback_latency_min", "valid_feedback"
    ]].copy()
    exclusions["reason"] = np.select(
        [~x.loc[exclusions.index, "eligible_outcome"], ~x.loc[exclusions.index, "valid_feedback"]],
        ["ineligible_conclusion", "invalid_or_missing_feedback_latency"], default="other"
    )
    return x, exclusions


def aggregate_ci(attempts: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    rows = []
    for group_key, group in attempts.groupby(keys, dropna=False):
        group_key = group_key if isinstance(group_key, tuple) else (group_key,)
        eligible = group[group["eligible_outcome"]]
        durations = eligible["feedback_latency_min"].dropna()
        logical = group.groupby("run_id")["attempt_number"].max()
        ordered = group.sort_values(["run_id", "attempt_number"])
        recovery = ordered.groupby("run_id").apply(
            lambda g: bool(g["conclusion_norm"].iloc[-1] == "success" and g["failure"].iloc[:-1].any()),
            include_groups=False,
        )
        queue_values = pd.to_numeric(
            eligible["queue_latency_min"], errors="coerce"
        ).dropna()
        execution_values = pd.to_numeric(
            eligible["execution_span_min"], errors="coerce"
        ).dropna()
        row = dict(zip(keys, group_key))
        row.update({
            "attempts_total": int(len(group)),
            "outcome_n": int(len(eligible)),
            "failure_count": int(eligible["failure"].sum()),
            "failure_rate": float(eligible["failure"].mean()) if len(eligible) else np.nan,
            "latency_n": int(len(durations)),
            "feedback_latency_median_min": float(durations.median()) if len(durations) else np.nan,
            "feedback_latency_p90_min": float(durations.quantile(.90)) if len(durations) else np.nan,
            "queue_latency_median_min": (
                float(queue_values.median())
                if not queue_values.empty
                else np.nan
            ),
            "execution_span_median_min": (
                float(execution_values.median())
                if not execution_values.empty
                else np.nan
            ),
            "logical_run_n": int(len(logical)),
            "rerun_count": int(logical.gt(1).sum()),
            "rerun_rate": float(logical.gt(1).mean()) if len(logical) else np.nan,
            "additional_attempt_count": int((logical - 1).clip(lower=0).sum()),
            "failed_then_passed_count": int(recovery.sum()),
            "failed_then_passed_rate_among_reruns": float(recovery.sum() / logical.gt(1).sum()) if logical.gt(1).sum() else np.nan,
        })
        rows.append(row)
    return pd.DataFrame(rows)


def build_pr_week(con: sqlite3.Connection, cfg: dict) -> pd.DataFrame:
    prs = read_sql(con, "SELECT * FROM pull_requests")
    reviews = read_sql(con, "SELECT repo_full, pr_number, user_login, user_type, state, submitted_at FROM pr_reviews")
    prs = prs[prs["repo_full"].isin(repositories(cfg))].copy()
    for col in ("created_at", "merged_at", "closed_at"):
        prs[col + "_ts"] = pd.to_datetime(prs[col], errors="coerce", utc=True)
    prs["cycle_hours"] = (prs["merged_at_ts"] - prs["created_at_ts"]).dt.total_seconds() / 3600
    reviews["submitted_ts"] = pd.to_datetime(reviews["submitted_at"], errors="coerce", utc=True)
    bot = reviews["user_type"].fillna("").str.lower().eq("bot") | reviews["user_login"].fillna("").str.lower().str.contains(r"\[bot\]$|bot$", regex=True)
    qualified = reviews["state"].fillna("").str.upper().isin(cfg["measurement"]["qualified_review_states"])
    first_review = reviews[~bot & qualified].groupby(["repo_full", "pr_number"], as_index=False)["submitted_ts"].min()
    prs = prs.merge(first_review, on=["repo_full", "pr_number"], how="left")
    prs["review_latency_hours"] = (prs["submitted_ts"] - prs["created_at_ts"]).dt.total_seconds() / 3600
    prs["week"] = monday(prs["merged_at_ts"])
    prs["churn"] = pd.to_numeric(prs["additions"], errors="coerce").fillna(0) + pd.to_numeric(prs["deletions"], errors="coerce").fillna(0)
    merged = prs[prs["merged_at_ts"].notna() & prs["week"].notna()].copy()
    merged_week = merged.groupby(["repo_full", "week"], as_index=False).agg(
        merged_pr_count=("pr_number", "size"),
        pr_cycle_hours_median=("cycle_hours", "median"),
        qualified_review_latency_hours_median=("review_latency_hours", "median"),
        reviewed_pr_n=("review_latency_hours", lambda s: int(s.notna().sum())),
        churn_median=("churn", "median"),
        changed_files_median=("changed_files", "median"),
        commits_median=("commits_count", "median"),
    )
    prs["created_week"] = monday(prs["created_at_ts"])
    created_week = prs[prs["created_week"].notna()].groupby(["repo_full", "created_week"], as_index=False).agg(
        created_pr_count=("pr_number", "size")
    ).rename(columns={"created_week": "week"})
    return merged_week.merge(created_week, on=["repo_full", "week"], how="outer")


def add_context(panel: pd.DataFrame, workflow: pd.DataFrame, con: sqlite3.Connection, cfg: dict, config_path: str) -> pd.DataFrame:
    result = panel.copy()
    concentration = workflow.groupby(["repo_full", "week"], as_index=False).apply(
        lambda g: pd.Series({
            "workflow_count": int(g["workflow_key"].nunique()),
            "workflow_concentration_hhi": float(np.square(g["attempts_total"] / max(g["attempts_total"].sum(), 1)).sum()),
        }), include_groups=False
    ).reset_index(drop=True)
    result = result.merge(concentration, on=["repo_full", "week"], how="left")
    try:
        releases = read_sql(con, "SELECT repo_full, published_at, created_at, draft FROM releases")
    except Exception:
        releases = pd.DataFrame()
    if releases.empty and cfg["paths"].get("releases_csv"):
        candidate = Path(cfg["paths"]["releases_csv"])
        if not candidate.is_absolute():
            candidate = Path(config_path).resolve().parent / candidate
        if candidate.is_file():
            releases = pd.read_csv(candidate, low_memory=False)
    if not releases.empty:
        timestamp = pd.to_datetime(releases["published_at"], errors="coerce", utc=True).fillna(pd.to_datetime(releases["created_at"], errors="coerce", utc=True))
        releases["week"] = monday(timestamp)
        if "draft" in releases:
            releases = releases[~releases["draft"].fillna(0).astype(bool)]
        counts = releases.groupby(["repo_full", "week"], as_index=False).size().rename(columns={"size": "release_count"})
        result = result.merge(counts, on=["repo_full", "week"], how="left")
    if "release_count" not in result:
        result["release_count"] = np.nan
    return result


def add_workflow_adjusted_latency(repository: pd.DataFrame, workflow: pd.DataFrame, cfg: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Estimate repository-specific workflow baselines from frozen references."""
    repo = repository.copy()
    wf = workflow.copy()
    wf["week"] = pd.to_datetime(wf["week"])
    wf["latency_log_raw"] = np.log1p(wf["feedback_latency_median_min"])
    reference = wf["split"].eq("train") | wf["external_calibration"]
    baseline = (
        wf[reference]
        .groupby(["repo_full", "workflow_key"], as_index=False)["latency_log_raw"]
        .median()
        .rename(columns={"latency_log_raw": "workflow_training_latency_log_median"})
    )
    wf = wf.merge(baseline, on=["repo_full", "workflow_key"], how="left")
    wf["workflow_latency_residual"] = wf["latency_log_raw"] - wf["workflow_training_latency_log_median"]
    weighted_rows = []
    for key, group in wf.groupby(["repo_full", "week"], dropna=False):
        valid = group["workflow_latency_residual"].notna() & group["latency_n"].gt(0)
        adjusted = np.average(group.loc[valid, "workflow_latency_residual"], weights=group.loc[valid, "latency_n"]) if valid.any() else np.nan
        weighted_rows.append({"repo_full": key[0], "week": key[1], "workflow_adjusted_latency_log": adjusted})
    repo = repo.merge(pd.DataFrame(weighted_rows), on=["repo_full", "week"], how="left")
    repo["latency_log_raw"] = np.log1p(repo["feedback_latency_median_min"])
    repo["latency_log"] = repo["workflow_adjusted_latency_log"]
    return repo, wf


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="analysis_config.yaml")
    args = parser.parse_args()
    cfg = load_config(args.config)
    database = sqlite_path(cfg, args.config)
    if not database.is_file():
        raise FileNotFoundError(database)
    out = output_dir(cfg, args.config)
    con = sqlite3.connect(database)
    check_tables(con)
    attempts, exclusions = build_attempt_table(con, cfg)
    repository = aggregate_ci(attempts, ["repo_full", "week"])
    workflow = aggregate_ci(attempts, ["repo_full", "workflow_key", "week"])
    weeks = pd.date_range(cfg["study"]["analysis_start"], cfg["study"]["holdout_end"], freq="W-MON")
    grid = pd.MultiIndex.from_product([repositories(cfg), weeks], names=["repo_full", "week"]).to_frame(index=False)
    repository = grid.merge(repository, on=["repo_full", "week"], how="left")
    repository = repository.merge(build_pr_week(con, cfg), on=["repo_full", "week"], how="left")
    repository = add_context(repository, workflow, con, cfg, args.config)
    count_cols = ["attempts_total", "outcome_n", "failure_count", "latency_n", "logical_run_n", "rerun_count", "additional_attempt_count", "failed_then_passed_count", "merged_pr_count", "created_pr_count", "reviewed_pr_n"]
    for col in count_cols:
        repository[col] = repository[col].fillna(0).astype(int)
    repository["split"] = assign_split(repository["week"], repository["repo_full"], cfg)
    workflow["split"] = assign_split(workflow["week"], workflow["repo_full"], cfg)
    repository = add_external_calibration_flags(repository, cfg)
    workflow = add_external_calibration_flags(workflow, cfg)
    repository, workflow = add_workflow_adjusted_latency(repository, workflow, cfg)
    repository.to_csv(out / "repository_week_panel.csv", index=False, date_format="%Y-%m-%d")
    workflow.to_csv(out / "workflow_week_panel.csv", index=False, date_format="%Y-%m-%d")
    attempts.to_csv(out / "attempt_measurements.csv.gz", index=False, compression="gzip", date_format="%Y-%m-%dT%H:%M:%SZ")
    exclusions.to_csv(out / "measurement_exclusions.csv.gz", index=False, compression="gzip")
    event_placeholders = ",".join("?" for _ in cfg["measurement"]["pr_validation_events"])
    run_counts = read_sql(con, f"SELECT repo_full, COUNT(*) AS eligible_event_runs FROM workflow_runs WHERE event IN ({event_placeholders}) GROUP BY repo_full", tuple(cfg["measurement"]["pr_validation_events"]))
    attempt_run_counts = attempts.groupby("repo_full")["run_id"].nunique().rename("runs_with_attempts").reset_index()
    coverage = run_counts.merge(attempt_run_counts, on="repo_full", how="left").fillna({"runs_with_attempts": 0})
    coverage["attempt_coverage"] = coverage["runs_with_attempts"] / coverage["eligible_event_runs"].replace(0, np.nan)
    metadata = {
        "database_sha256": sha256(database),
        "database_bytes": database.stat().st_size,
        "attempt_rows": len(attempts),
        "repository_week_rows": len(repository),
        "workflow_week_rows": len(workflow),
        "measurement_definition": "attempt created_at to latest completed job; logical runs assigned to original run week",
        "external_calibration_policy": {
            "weeks": int(cfg["study"]["external_calibration_weeks"]),
            "definition": "first observed analysis weeks per external repository",
            "uses": "repository-specific unsupervised centering and scaling only",
            "excluded_from_external_evaluation": True,
        },
        "attempt_coverage_by_repository": coverage.to_dict(orient="records"),
    }
    write_json(out / "measurement_metadata.json", metadata)
    print(json.dumps(metadata, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())