import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = PROJECT_ROOT / "data" / "raw"
FIG_DIR = PROJECT_ROOT / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Load PR data
# -----------------------------
prs = pd.read_csv(RAW / "prs.csv")

# -----------------------------
# Filter merged PRs
# -----------------------------
prs["merged_at_dt"] = pd.to_datetime(prs["merged_at"], utc=True, errors="coerce")
prs["is_merged"] = prs["state"].eq("MERGED")
prs = prs[prs["is_merged"]].dropna(subset=["merged_at_dt", "repo_full"])

# -----------------------------
# Week bucket (Monday start, pandas-safe)
# -----------------------------
prs["week"] = (
    prs["merged_at_dt"]
      .dt.tz_convert(None)
      .dt.to_period("W-MON")
      .dt.start_time
)

# -----------------------------
# Aggregate: merge frequency per week
# -----------------------------
weekly = (
    prs.groupby(["repo_full", "week"], as_index=False)
       .agg(merge_count=("pr_number", "count"))
       .sort_values("week")
)

# -----------------------------
# Optional: 4-week rolling average (recommended)
# -----------------------------
weekly["merge_count_smooth"] = (
    weekly.groupby("repo_full")["merge_count"]
          .transform(lambda s: s.rolling(4, min_periods=2).mean())
)

# -----------------------------
# FIGURE: Merge Frequency Weekly
# -----------------------------
fig, ax = plt.subplots(figsize=(11, 5))

for repo, sub in weekly.groupby("repo_full"):
    sub = sub.sort_values("week")
    ax.plot(
        sub["week"],
        sub["merge_count"],
        linewidth=1,
        alpha=0.5,
        label=f"{repo} (weekly)"
    )
    ax.plot(
        sub["week"],
        sub["merge_count_smooth"],
        linewidth=2.5,
        label=f"{repo} (4-week avg)"
    )

ax.set_title("Merge Frequency per Week")
ax.set_xlabel("Week")
ax.set_ylabel("Merged Pull Requests")
ax.grid(True, alpha=0.3)
ax.legend()
plt.tight_layout()

out = FIG_DIR / "Figure_Merge_Frequency_Weekly.png"
plt.savefig(out, dpi=300, bbox_inches="tight")
print("Saved:", out)
plt.close(fig)

print("Done.")
