from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "raw"
FIG_DIR = PROJECT_ROOT / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

prs = pd.read_csv(DATA_DIR / "prs.csv")

prs["created_at_dt"] = pd.to_datetime(prs["created_at"], utc=True, errors="coerce")
prs["week"] = prs["created_at_dt"].dt.to_period("W").dt.start_time

m = prs.dropna(subset=["pr_cycle_hours"]).copy()

weekly = (m.groupby(["repo", "week"])["pr_cycle_hours"]
          .median()
          .reset_index()
          .sort_values("week"))

fig, ax = plt.subplots(figsize=(11, 5))
for repo, sub in weekly.groupby("repo"):
    ax.plot(sub["week"], sub["pr_cycle_hours"], label=repo)

ax.set_ylabel("Median PR Cycle Time (hours)")
ax.set_xlabel("Week")
ax.set_title("Figure 3: PR Cycle Time Over Time (Median per Week)")
ax.legend()
plt.tight_layout()

out = FIG_DIR / "PR_Cycle.png"
plt.savefig(out, dpi=300, bbox_inches="tight")
print("Saved:", out)

plt.show()
