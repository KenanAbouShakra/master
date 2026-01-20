import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
OUT = PROJECT_ROOT / "data" / "derived"
OUT.mkdir(parents=True, exist_ok=True)

prs = pd.read_csv(RAW / "prs.csv")

prs["pr_churn"] = prs["additions"].fillna(0) + prs["deletions"].fillna(0)

out = prs[["repo_full","pr_number","created_at","merged_at","additions","deletions","pr_churn"]].copy()
out.to_csv(OUT / "pr_churn_pr_level.csv", index=False)
print("Saved:", OUT / "pr_churn_pr_level.csv")
