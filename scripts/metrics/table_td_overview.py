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

# ---------- PR Churn (median) ----------
pr_churn_path = DERIVED / "pr_churn_pr_level.csv"
if not pr_churn_path.exists():
    # fallback: compute from prs.csv
    prs_path = RAW / "prs.csv"
    if not prs_path.exists():
        raise FileNotFoundError("Need pr_churn_pr_level.csv OR raw prs.csv to compute PR churn.")
    prs = pd.read_csv(prs_path)
    if not {"repo_full", "additions", "deletions"}.issubset(prs.columns):
        raise ValueError("prs.csv missing additions/deletions to compute churn.")
    prs["pr_churn"] = prs["additions"].fillna(0) + prs["deletions"].fillna(0)
    pr_churn_repo = (
        prs.groupby("repo_full", as_index=False)["pr_churn"]
           .median()
           .rename(columns={"pr_churn": "PR Churn (median)"})
    )
else:
    prc = pd.read_csv(pr_churn_path)
    if not {"repo_full", "pr_churn"}.issubset(prc.columns):
        raise ValueError("pr_churn_pr_level.csv missing required columns (repo_full, pr_churn).")
    pr_churn_repo = (
        prc.groupby("repo_full", as_index=False)["pr_churn"]
           .median()
           .rename(columns={"pr_churn": "PR Churn (median)"})
    )

# ---------- CI Flakiness (avg runs per SHA) ----------
flaky_path = DERIVED / "ci_flakiness_retry_weekly.csv"
if not flaky_path.exists():
    raise FileNotFoundError(f"Missing: {flaky_path} (run ci_flakiness script first)")

flaky = pd.read_csv(flaky_path)
col_flaky = pick_col(flaky, ["avg_runs_per_sha", "runs_per_sha_avg", "avg_runs_sha"])
if not col_flaky or "repo_full" not in flaky.columns:
    raise ValueError("ci_flakiness_retry_weekly.csv missing repo_full and avg_runs_per_sha (or equivalent).")

ci_flaky_repo = (
    flaky.groupby("repo_full", as_index=False)[col_flaky]
         .mean()
         .rename(columns={col_flaky: "CI Flakiness (avg runs/SHA)"})
)

# ---------- Review Overhead (median hours) ----------
# Prefer derived review_overhead_weekly.csv; fallback to compute from prs.csv
review_overhead_repo = None

ro_path = DERIVED / "review_overhead_weekly.csv"
if ro_path.exists():
    ro = pd.read_csv(ro_path)
    # use review latency median if available, else review duration median
    col_over = pick_col(ro, ["review_latency_med_h", "review_latency_median_h", "review_duration_med_h", "review_duration_median_h"])
    if col_over and "repo_full" in ro.columns:
        review_overhead_repo = (
            ro.groupby("repo_full", as_index=False)[col_over]
              .median()
              .rename(columns={col_over: "Review Overhead (median hours)"})
        )

if review_overhead_repo is None:
    prs_path = RAW / "prs.csv"
    if not prs_path.exists():
        review_overhead_repo = pd.DataFrame(columns=["repo_full", "Review Overhead (median hours)"])
    else:
        prs = pd.read_csv(prs_path)
        if not {"repo_full", "created_at", "first_review_at"}.issubset(prs.columns):
            review_overhead_repo = pd.DataFrame(columns=["repo_full", "Review Overhead (median hours)"])
        else:
            prs["created_at_dt"] = pd.to_datetime(prs["created_at"], utc=True, errors="coerce")
            prs["first_review_dt"] = pd.to_datetime(prs["first_review_at"], utc=True, errors="coerce")
            prs["review_latency_h"] = (prs["first_review_dt"] - prs["created_at_dt"]).dt.total_seconds() / 3600.0
            review_overhead_repo = (
                prs.dropna(subset=["review_latency_h"])
                   .groupby("repo_full", as_index=False)["review_latency_h"]
                   .median()
                   .rename(columns={"review_latency_h": "Review Overhead (median hours)"})
            )

# ---------- Sonar Debt Ratio (median) ----------
sonar_path = DERIVED / "sonar_snapshots_tidy.csv"
if sonar_path.exists():
    sonar = pd.read_csv(sonar_path)
    col_debt = pick_col(sonar, ["sqale_debt_ratio", "debt_ratio"])
    if col_debt and "repo_full" in sonar.columns:
        sonar_repo = (
            sonar.groupby("repo_full", as_index=False)[col_debt]
                 .median()
                 .rename(columns={col_debt: "Sonar Debt Ratio (median)"})
        )
    else:
        sonar_repo = pd.DataFrame(columns=["repo_full", "Sonar Debt Ratio (median)"])
else:
    sonar_repo = pd.DataFrame(columns=["repo_full", "Sonar Debt Ratio (median)"])

# ---------- Merge Table B ----------
tabB = pr_churn_repo.merge(ci_flaky_repo, on="repo_full", how="outer")
tabB = tabB.merge(review_overhead_repo, on="repo_full", how="outer")
tabB = tabB.merge(sonar_repo, on="repo_full", how="outer")

tabB = tabB.rename(columns={"repo_full": "Repo"}).sort_values("Repo")

out_path = DERIVED / "Table_B_technical_debt_overview.csv"
tabB.to_csv(out_path, index=False)
print("Saved:", out_path)
print("\nTable B preview:\n", tabB.to_string(index=False))
