from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

# -----------------------------
# Paths (robust: find root by locating /data)
# -----------------------------
p = Path(__file__).resolve()
PROJECT_ROOT = next(parent for parent in p.parents if (parent / "data").exists())

DATA_DIR = PROJECT_ROOT / "data" / "raw"
FIG_DIR = PROJECT_ROOT / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Load data
# -----------------------------
prs_path = DATA_DIR / "prs.csv"
if not prs_path.exists():
    raise FileNotFoundError(f"Could not find {prs_path}")

prs = pd.read_csv(prs_path)

# -----------------------------
# Ensure review_latency_hours exists
# -----------------------------
if "review_latency_hours" not in prs.columns:
    prs["created_at_dt"] = pd.to_datetime(prs["created_at"], utc=True, errors="coerce")
    prs["first_review_dt"] = pd.to_datetime(prs["first_review_at"], utc=True, errors="coerce")
    prs["review_latency_hours"] = (prs["first_review_dt"] - prs["created_at_dt"]).dt.total_seconds() / 3600.0

# -----------------------------
# Prepare data per repo
# -----------------------------
repo_col = "repo_full" if "repo_full" in prs.columns else "repo"

data = []
labels = []

for repo in sorted(prs[repo_col].dropna().unique()):
    vals = prs.loc[prs[repo_col] == repo, "review_latency_hours"].dropna()
    if len(vals) > 0:
        data.append(vals)
        labels.append(repo)

if not data:
    raise RuntimeError("No review latency data found (all values are NaN).")

# -----------------------------
# Plot
# -----------------------------
fig, ax = plt.subplots(figsize=(8, 5))
ax.boxplot(data, labels=labels, showfliers=False)

ax.set_ylabel("Review Latency (hours)")
ax.set_title("Review Latency Distribution by Project")

plt.tight_layout()

# -----------------------------
# Save figure
# -----------------------------
out = FIG_DIR / "Figure_Review_Latency.png"
plt.savefig(out, dpi=300, bbox_inches="tight")
print("Saved:", out)

plt.show()
