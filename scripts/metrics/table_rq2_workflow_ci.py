import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
DERIVED = PROJECT_ROOT / "data" / "derived"
DERIVED.mkdir(parents=True, exist_ok=True)


def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def safe_datetime(series):
    return pd.to_datetime(series, utc=True, errors="coerce")


# ---------------------------------------------------
# 1) PR DATA: Median PR Cycle Time, Review Overhead, PR Churn
# ---------------------------------------------------
prs_path = RAW / "prs.csv"
if not prs_path.exists():
    raise FileNotFoundError(f"Missing raw file: {prs_path}")

prs = pd.read_csv(prs_path)

required_pr_cols = {"repo_full", "created_at"}
missing_pr = required_pr_cols - set(prs.columns)
if missing_pr:
    raise ValueError(f"prs.csv missing required columns: {sorted(missing_pr)}")

prs["created_at_dt"] = safe_datetime(prs["created_at"])

if "merged_at" in prs.columns:
    prs["merged_at_dt"] = safe_datetime(prs["merged_at"])
else:
    prs["merged_at_dt"] = pd.NaT

if "closed_at" in prs.columns:
    prs["closed_at_dt"] = safe_datetime(prs["closed_at"])
else:
    prs["closed_at_dt"] = pd.NaT

prs["done_at_dt"] = prs["merged_at_dt"].fillna(prs["closed_at_dt"])
prs["pr_cycle_hours"] = (
    (prs["done_at_dt"] - prs["created_at_dt"]).dt.total_seconds() / 3600.0
)

# Use merged PRs where possible for cycle time counts
if "merged_at" in prs.columns:
    prs_cycle = prs.dropna(subset=["created_at_dt", "merged_at_dt", "pr_cycle_hours"]).copy()
else:
    prs_cycle = prs.dropna(subset=["created_at_dt", "done_at_dt", "pr_cycle_hours"]).copy()

pr_cycle_repo = (
    prs_cycle.groupby("repo_full", as_index=False)
    .agg(
        pr_cycle_median_h=("pr_cycle_hours", "median"),
        pr_cycle_iqr_h=("pr_cycle_hours", lambda s: s.quantile(0.75) - s.quantile(0.25)),
        merged_prs_n=("pr_cycle_hours", "count"),
        pr_window_start=("created_at_dt", "min"),
        pr_window_end=("created_at_dt", "max"),
    )
)

# Review Overhead = median review latency from PR creation to first review
if "first_review_at" in prs.columns:
    prs["first_review_dt"] = safe_datetime(prs["first_review_at"])
    prs["review_overhead_h"] = (
        (prs["first_review_dt"] - prs["created_at_dt"]).dt.total_seconds() / 3600.0
    )

    review_repo = (
        prs.dropna(subset=["review_overhead_h"])
        .groupby("repo_full", as_index=False)
        .agg(
            review_overhead_median_h=("review_overhead_h", "median"),
            review_overhead_iqr_h=("review_overhead_h", lambda s: s.quantile(0.75) - s.quantile(0.25)),
            reviewed_prs_n=("review_overhead_h", "count"),
        )
    )
else:
    review_repo = pd.DataFrame(columns=[
        "repo_full", "review_overhead_median_h", "review_overhead_iqr_h", "reviewed_prs_n"
    ])

# PR Churn = additions + deletions
if {"additions", "deletions"}.issubset(prs.columns):
    prs["pr_churn"] = prs["additions"].fillna(0) + prs["deletions"].fillna(0)

    churn_repo = (
        prs.dropna(subset=["pr_churn"])
        .groupby("repo_full", as_index=False)
        .agg(
            pr_churn_median=("pr_churn", "median"),
            pr_churn_iqr=("pr_churn", lambda s: s.quantile(0.75) - s.quantile(0.25)),
            churn_prs_n=("pr_churn", "count"),
        )
    )
else:
    churn_repo = pd.DataFrame(columns=[
        "repo_full", "pr_churn_median", "pr_churn_iqr", "churn_prs_n"
    ])


# ---------------------------------------------------
# 2) CI DATA: Median CI Duration, CI Failure Rate, CI Flakiness
# ---------------------------------------------------
ci_weekly_path = DERIVED / "ci_weekly.csv"
flaky_weekly_path = DERIVED / "ci_flakiness_retry_weekly.csv"
workflow_runs_path = RAW / "workflow_runs.csv"

ci_repo = None
flaky_repo = None

if ci_weekly_path.exists():
    ci = pd.read_csv(ci_weekly_path)

    if "repo_full" not in ci.columns:
        raise ValueError("ci_weekly.csv must contain repo_full")

    ci_duration_col = pick_col(ci, ["ci_duration_med_min", "ci_duration_median_min"])
    ci_failure_col = pick_col(ci, ["ci_failure_rate"])

    if ci_duration_col is None or ci_failure_col is None:
        raise ValueError("ci_weekly.csv missing CI duration median or failure rate columns")

    ci_repo = (
        ci.groupby("repo_full", as_index=False)
        .agg(
            ci_duration_median_min=(ci_duration_col, "median"),
            ci_failure_rate_mean=(ci_failure_col, "mean"),
            ci_failure_rate_median=(ci_failure_col, "median"),
        )
    )

if flaky_weekly_path.exists():
    flaky = pd.read_csv(flaky_weekly_path)

    flaky_col = pick_col(flaky, ["avg_runs_per_sha", "runs_per_sha_avg", "avg_runs_sha"])
    if flaky_col is None or "repo_full" not in flaky.columns:
        raise ValueError("ci_flakiness_retry_weekly.csv missing flakiness column or repo_full")

    flaky_repo = (
        flaky.groupby("repo_full", as_index=False)
        .agg(
            ci_flakiness_mean=(flaky_col, "mean"),
            ci_flakiness_median=(flaky_col, "median"),
        )
    )

# Fallback: compute directly from workflow_runs.csv if derived files are absent
if ci_repo is None or flaky_repo is None:
    if not workflow_runs_path.exists():
        raise FileNotFoundError(
            "Need ci_weekly.csv / ci_flakiness_retry_weekly.csv or raw workflow_runs.csv"
        )

    wr = pd.read_csv(workflow_runs_path)

    if "repo_full" not in wr.columns:
        raise ValueError("workflow_runs.csv missing repo_full")

    duration_col = pick_col(
        wr,
        ["duration_min", "run_duration_min", "ci_duration_min", "duration_minutes"]
    )
    conclusion_col = pick_col(wr, ["conclusion", "run_conclusion"])
    sha_col = pick_col(wr, ["head_sha", "sha"])

    if ci_repo is None:
        if duration_col is None or conclusion_col is None:
            raise ValueError("workflow_runs.csv missing duration and/or conclusion columns")

        wr["is_failure"] = wr[conclusion_col].astype(str).str.lower().isin(
            ["failure", "failed", "timed_out", "cancelled", "startup_failure", "action_required"]
        )

        ci_repo = (
            wr.dropna(subset=[duration_col])
            .groupby("repo_full", as_index=False)
            .agg(
                ci_duration_median_min=(duration_col, "median"),
                ci_failure_rate_mean=("is_failure", "mean"),
                ci_runs_n=("is_failure", "count"),
            )
        )

    if flaky_repo is None:
        if sha_col is None:
            flaky_repo = pd.DataFrame(columns=["repo_full", "ci_flakiness_mean", "ci_flakiness_median"])
        else:
            runs_per_sha = (
                wr.dropna(subset=[sha_col])
                .groupby(["repo_full", sha_col], as_index=False)
                .size()
                .rename(columns={"size": "runs_per_sha"})
            )

            flaky_repo = (
                runs_per_sha.groupby("repo_full", as_index=False)
                .agg(
                    ci_flakiness_mean=("runs_per_sha", "mean"),
                    ci_flakiness_median=("runs_per_sha", "median"),
                )
            )


# ---------------------------------------------------
# 3) TABLE 1: WORKFLOW + PR CHURN
# ---------------------------------------------------
table1 = pr_cycle_repo.copy()
table1 = table1.merge(review_repo, on="repo_full", how="left")
table1 = table1.merge(churn_repo, on="repo_full", how="left")

table1 = table1.rename(columns={
    "repo_full": "Repository",
    "pr_cycle_median_h": "Median PR Cycle Time (h)",
    "pr_cycle_iqr_h": "PR Cycle Time IQR (h)",
    "merged_prs_n": "Merged PRs (N)",
    "review_overhead_median_h": "Review Overhead (median h)",
    "review_overhead_iqr_h": "Review Overhead IQR (h)",
    "reviewed_prs_n": "Reviewed PRs (N)",
    "pr_churn_median": "PR Churn (median)",
    "pr_churn_iqr": "PR Churn IQR",
    "churn_prs_n": "PR Churn PRs (N)",
})

if {"pr_window_start", "pr_window_end"}.issubset(table1.columns):
    table1["PR Analysis Window"] = (
        pd.to_datetime(table1["pr_window_start"]).dt.strftime("%Y-%m")
        + " to "
        + pd.to_datetime(table1["pr_window_end"]).dt.strftime("%Y-%m")
    )
    table1 = table1.drop(columns=["pr_window_start", "pr_window_end"])

for col in table1.columns:
    if col != "Repository" and pd.api.types.is_numeric_dtype(table1[col]):
        table1[col] = table1[col].round(2)

table1 = table1.sort_values("Repository")

out_csv_1 = DERIVED / "Table_RQ2_Workflow_Churn.csv"
table1.to_csv(out_csv_1, index=False)

print("Saved:", out_csv_1)
print("\nTable 1: Workflow + PR churn\n")
print(table1.to_string(index=False, line_width=2000))



# ---------------------------------------------------
# 4) TABLE 2: CI/CD METRICS
# ---------------------------------------------------
table2 = ci_repo.copy()
table2 = table2.merge(flaky_repo, on="repo_full", how="left")

table2 = table2.rename(columns={
    "repo_full": "Repository",
    "ci_duration_median_min": "Median CI Duration (min)",
    "ci_failure_rate_mean": "CI Failure Rate (mean)",
    "ci_failure_rate_median": "CI Failure Rate (median)",
    "ci_flakiness_mean": "CI Flakiness (mean runs/SHA)",
    "ci_flakiness_median": "CI Flakiness (median runs/SHA)",
    "ci_runs_n": "CI Runs (N)",
})

for col in table2.columns:
    if col != "Repository" and pd.api.types.is_numeric_dtype(table2[col]):
        table2[col] = table2[col].round(2)

table2 = table2.sort_values("Repository")

out_csv_2 = DERIVED / "Table_RQ2_CI_Metrics.csv"
table2.to_csv(out_csv_2, index=False)

print("\nSaved:", out_csv_2)
print("\nTable 2: CI/CD metrics\n")
print(table2.to_string(index=False, line_width=2000))