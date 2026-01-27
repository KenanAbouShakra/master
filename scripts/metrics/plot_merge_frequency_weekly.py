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
prs_path = RAW / "prs.csv"
if not prs_path.exists():
    raise FileNotFoundError(f"Could not find {prs_path}")

prs = pd.read_csv(prs_path)

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
       .sort_values(["repo_full", "week"])
)

# -----------------------------
#4-week rolling average
# -----------------------------
weekly["merge_count_smooth"] = (
    weekly.groupby("repo_full")["merge_count"]
          .transform(lambda s: s.rolling(4, min_periods=2).mean())
)

# -----------------------------
# FIGURE 1: Faceted (one row per repo) - weekly + 4w avg
# -----------------------------
repos = sorted(weekly["repo_full"].unique())

fig, axes = plt.subplots(len(repos), 1, figsize=(11, 9), sharex=True)
if len(repos) == 1:
    axes = [axes]

for ax, repo in zip(axes, repos):
    sub = weekly[weekly["repo_full"] == repo].sort_values("week")

    ax.plot(sub["week"], sub["merge_count"], linewidth=1, alpha=0.5, label="Weekly")
    ax.plot(sub["week"], sub["merge_count_smooth"], linewidth=2.5, label="4-week avg")

    ax.set_title(repo)
    ax.set_ylabel("Merged PRs")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left")

axes[-1].set_xlabel("Week")
fig.suptitle("Merge Frequency per Week (Weekly and 4-week Average)", y=1.02)
plt.tight_layout()

out1 = FIG_DIR / "Figure_Merge_Frequency_Faceted.png"
plt.savefig(out1, dpi=300, bbox_inches="tight")
print("Saved:", out1)
plt.close(fig)

# -----------------------------
# FIGURE 2: All repos (4w avg only) - comparison plot
# -----------------------------
fig, ax = plt.subplots(figsize=(11, 5))

for repo, sub in weekly.groupby("repo_full"):
    sub = sub.sort_values("week")
    ax.plot(
        sub["week"],
        sub["merge_count_smooth"],
        linewidth=2.5,
        label=repo
    )

ax.set_title("Merge Frequency per Week (4-week Average Only)")
ax.set_xlabel("Week")
ax.set_ylabel("Merged Pull Requests (4-week avg)")
ax.grid(True, alpha=0.3)
ax.legend(loc="upper left")
plt.tight_layout()

out2 = FIG_DIR / "Figure_Merge_Frequency_4wAvg_Only_Comparison.png"
plt.savefig(out2, dpi=300, bbox_inches="tight")
print("Saved:", out2)
plt.close(fig)

print("Done.")