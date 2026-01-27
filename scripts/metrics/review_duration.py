import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
OUT = PROJECT_ROOT / "data" / "derived"
OUT.mkdir(parents=True, exist_ok=True)

prs = pd.read_csv(RAW / "prs.csv")

prs["created_at_dt"] = pd.to_datetime(prs["created_at"], utc=True, errors="coerce")
prs["merged_at_dt"] = pd.to_datetime(prs["merged_at"], utc=True, errors="coerce")
prs["closed_at_dt"] = pd.to_datetime(prs["closed_at"], utc=True, errors="coerce")
prs["first_review_dt"] = pd.to_datetime(prs["first_review_at"], utc=True, errors="coerce")

prs["done_at_dt"] = prs["merged_at_dt"].fillna(prs["closed_at_dt"])
prs["review_duration_hours"] = (prs["done_at_dt"] - prs["first_review_dt"]).dt.total_seconds() / 3600.0

out = prs[["repo_full","pr_number","created_at_dt","first_review_dt","done_at_dt","review_duration_hours"]].copy()
out.to_csv(OUT / "review_duration_pr_level.csv", index=False)
print("Saved:", OUT / "review_duration_pr_level.csv")
