import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
DERIVED = PROJECT_ROOT / "data" / "derived"
DERIVED.mkdir(parents=True, exist_ok=True)

# ---------- Load CI weekly ----------
ci_path = DERIVED / "ci_weekly.csv"
if not ci_path.exists():
    raise FileNotFoundError(f"Missing: {ci_path} (run CI script first)")

ci = pd.read_csv(ci_path)

need_ci = {"repo_full", "ci_duration_med_min", "ci_failure_rate"}
miss = need_ci - set(ci.columns)
if miss:
    raise ValueError(f"ci_weekly.csv missing columns: {sorted(miss)}")

ci_repo = (
    ci.groupby("repo_full", as_index=False)
      .agg(
          **{
              "Median CI Duration (min)": ("ci_duration_med_min", "median"),
              "Median Failure Rate": ("ci_failure_rate", "median"),
          }
      )
)

# ---------- Get PR Cycle median per repo ----------
def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

pr_cycle_repo = None

# Option 1: pr_cycle_weekly.csv
pr_cycle_weekly_path = DERIVED / "pr_cycle_weekly.csv"
if pr_cycle_weekly_path.exists():
    pcw = pd.read_csv(pr_cycle_weekly_path)
    col = pick_col(pcw, ["pr_cycle_med_h", "pr_cycle_hours_med", "pr_cycle_med", "pr_cycle_median_h"])
    if col and "repo_full" in pcw.columns:
        pr_cycle_repo = (
            pcw.groupby("repo_full", as_index=False)[col]
               .median()
               .rename(columns={col: "Median PR Cycle (h)"})
        )

# Option 2: review_overhead_weekly.csv
if pr_cycle_repo is None:
    ro_path = DERIVED / "review_overhead_weekly.csv"
    if ro_path.exists():
        ro = pd.read_csv(ro_path)
        col = pick_col(ro, ["pr_cycle_med_h", "pr_cycle_hours_med", "pr_cycle_med", "pr_cycle_median_h"])
        if col and "repo_full" in ro.columns:
            pr_cycle_repo = (
                ro.groupby("repo_full", as_index=False)[col]
                  .median()
                  .rename(columns={col: "Median PR Cycle (h)"})
            )

# Option 3: compute from raw prs.csv
if pr_cycle_repo is None:
    prs_path = RAW / "prs.csv"
    if not prs_path.exists():
        raise FileNotFoundError("Could not find PR cycle in derived files, and raw prs.csv is missing.")

    prs = pd.read_csv(prs_path)

    for c in ["created_at", "merged_at", "closed_at", "state", "repo_full"]:
        if c not in prs.columns:
            raise ValueError(f"prs.csv missing required column: {c}")

    prs["created_at_dt"] = pd.to_datetime(prs["created_at"], utc=True, errors="coerce")
    prs["merged_at_dt"] = pd.to_datetime(prs["merged_at"], utc=True, errors="coerce")
    prs["closed_at_dt"] = pd.to_datetime(prs["closed_at"], utc=True, errors="coerce")

    prs["done_at_dt"] = prs["merged_at_dt"].fillna(prs["closed_at_dt"])
    prs["pr_cycle_hours"] = (prs["done_at_dt"] - prs["created_at_dt"]).dt.total_seconds() / 3600.0

    pr_cycle_repo = (
        prs.dropna(subset=["pr_cycle_hours"])
           .groupby("repo_full", as_index=False)["pr_cycle_hours"]
           .median()
           .rename(columns={"pr_cycle_hours": "Median PR Cycle (h)"})
    )

# ---------- Merge Table A ----------
tabA = (
    ci_repo.merge(pr_cycle_repo, on="repo_full", how="left")
           .rename(columns={"repo_full": "Repository"})
           .sort_values("Repository")
)

out_path = DERIVED / "Table_A_repo_comparison.csv"
tabA.to_csv(out_path, index=False)
print("Saved:", out_path)
print("\nTable A preview:\n", tabA.to_string(index=False))
