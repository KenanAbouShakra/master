import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA = PROJECT_ROOT / "data" / "derived"
FIG = PROJECT_ROOT / "figures"
FIG.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Load data
# -----------------------------
path = DATA / "review_overhead_weekly.csv"
if not path.exists():
    raise FileNotFoundError(f"Missing: {path}")

df = pd.read_csv(path)

# -----------------------------
# Validate required columns
# -----------------------------
required = {"repo_full", "week", "review_duration_med_h"}
missing = required - set(df.columns)
if missing:
    raise ValueError(f"Missing columns in {path.name}: {sorted(missing)}")

df["week"] = pd.to_datetime(df["week"], utc=True, errors="coerce")
df = df.dropna(subset=["week", "review_duration_med_h", "repo_full"]).sort_values(["repo_full", "week"])

if df.empty:
    raise SystemExit("No valid rows after parsing week/review_duration_med_h.")

# -----------------------------
# 4-week rolling median smoothing (recommended)
# -----------------------------
df["review_duration_smooth"] = (
    df.groupby("repo_full")["review_duration_med_h"]
      .transform(lambda s: s.rolling(4, min_periods=2).median())
)

# -----------------------------
# FIGURE 1: Faceted (one row per repo)
# -----------------------------
repos = sorted(df["repo_full"].unique())
fig, axes = plt.subplots(len(repos), 1, figsize=(11, 9), sharex=True)

# Handle single-repo case
if len(repos) == 1:
    axes = [axes]

for ax, repo in zip(axes, repos):
    sub = df[df["repo_full"] == repo].sort_values("week")

    # Weekly median (lighter)
    ax.plot(sub["week"], sub["review_duration_med_h"], alpha=0.3, linewidth=1, label="Weekly median")

    # Smoothed median (main signal)
    ax.plot(sub["week"], sub["review_duration_smooth"], linewidth=2.5, label="4-week median")

    ax.set_title(repo)
    ax.set_ylabel("Hours")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left")

axes[-1].set_xlabel("Week")
fig.suptitle("Review Duration Over Time (Weekly Median and 4-week Smoothed)", y=1.02)
plt.tight_layout()

out1 = FIG / "Figure_Review_Duration_Faceted_Weekly_4wMedian.png"
plt.savefig(out1, dpi=300, bbox_inches="tight")
print("Saved:", out1)
plt.close(fig)

# -----------------------------
# FIGURE 2: All repos together (4-week median only)
# -----------------------------
fig, ax = plt.subplots(figsize=(11, 5))
for repo, sub in df.groupby("repo_full"):
    sub = sub.sort_values("week")
    ax.plot(sub["week"], sub["review_duration_smooth"], linewidth=2.5, label=repo)

ax.set_ylabel("Review Duration (hours)")
ax.set_xlabel("Week")
ax.set_title("Review Duration Over Time (4-week Median Only)")
ax.grid(True, alpha=0.3)
ax.legend(loc="upper left")
plt.tight_layout()

out2 = FIG / "Figure_Review_Duration_4wMedian_Only_Comparison.png"
plt.savefig(out2, dpi=300, bbox_inches="tight")
print("Saved:", out2)
plt.close(fig)

print("Done.")