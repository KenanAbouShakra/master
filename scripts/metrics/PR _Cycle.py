from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

# -----------------------------
# Paths
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "raw"
FIG_DIR = PROJECT_ROOT / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Load PR data
# -----------------------------
prs_path = DATA_DIR / "prs.csv"
if not prs_path.exists():
    raise FileNotFoundError(f"Missing: {prs_path}")

prs = pd.read_csv(prs_path)

# -----------------------------
# Parse timestamps
# -----------------------------
prs["created_at_dt"] = pd.to_datetime(prs["created_at"], utc=True, errors="coerce")
prs = prs.dropna(subset=["created_at_dt"])

# -----------------------------
# Week bucket (Monday start, stable)
# -----------------------------
prs["week"] = (
    prs["created_at_dt"]
      .dt.tz_convert(None)
      .dt.to_period("W-MON")
      .dt.start_time
)

# -----------------------------
# Ensure PR cycle time column exists
# -----------------------------
if "pr_cycle_hours" not in prs.columns:
    raise ValueError("Missing column: pr_cycle_hours (expected to exist in prs.csv)")

m = prs.dropna(subset=["pr_cycle_hours"]).copy()

# Repo column: prefer repo_full if available, else repo
repo_col = "repo_full" if "repo_full" in m.columns else ("repo" if "repo" in m.columns else None)
if repo_col is None:
    raise ValueError("Missing repo identifier column (expected repo_full or repo).")

# -----------------------------
# Aggregate: median PR cycle time per repo/week
# -----------------------------
weekly = (
    m.groupby([repo_col, "week"], as_index=False)
     .agg(pr_cycle_hours=("pr_cycle_hours", "median"))
     .sort_values([repo_col, "week"])
)

# -----------------------------
# 4-week rolling median (smoothing)
# -----------------------------
weekly["pr_cycle_smooth"] = (
    weekly.groupby(repo_col)["pr_cycle_hours"]
          .transform(lambda s: s.rolling(4, min_periods=2).median())
)

# -----------------------------
# FIGURE 1: Faceted plot (one row per repo)
# -----------------------------
repos = sorted(weekly[repo_col].unique())
fig, axes = plt.subplots(len(repos), 1, figsize=(11, 9), sharex=True)

# Handle single-repo case
if len(repos) == 1:
    axes = [axes]

for ax, repo in zip(axes, repos):
    sub = weekly[weekly[repo_col] == repo].sort_values("week")

    # Weekly median (lighter)
    ax.plot(sub["week"], sub["pr_cycle_hours"], alpha=0.3, linewidth=1, label="Weekly median")

    # Smoothed median (main signal)
    ax.plot(sub["week"], sub["pr_cycle_smooth"], linewidth=2.5, label="4-week median")

    ax.set_title(repo)
    ax.set_ylabel("Hours")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left")

axes[-1].set_xlabel("Week")
fig.suptitle("PR Cycle Time Over Time (Median, Weekly and 4-week Smoothed)", y=1.02)
plt.tight_layout()

out = FIG_DIR / "Figure_PR_Cycle_Time_Faceted_Weekly_4wMedian.png"
plt.savefig(out, dpi=300, bbox_inches="tight")
print("Saved:", out)
plt.close(fig)

# -----------------------------
# FIGURE 2: All repos together (4-week median only)
# -----------------------------
fig, ax = plt.subplots(figsize=(11, 5))
for repo, sub in weekly.groupby(repo_col):
    sub = sub.sort_values("week")
    ax.plot(sub["week"], sub["pr_cycle_smooth"], linewidth=2.5, label=repo)

ax.set_title("PR Cycle Time Over Time (4-week Median Only)")
ax.set_xlabel("Week")
ax.set_ylabel("PR Cycle Time (hours)")
ax.grid(True, alpha=0.3)
ax.legend(loc="upper left")
plt.tight_layout()

out2 = FIG_DIR / "Figure_PR_Cycle_Time_4wMedian_Only_Comparison.png"
plt.savefig(out2, dpi=300, bbox_inches="tight")
print("Saved:", out2)
plt.close(fig)

print("Done.")