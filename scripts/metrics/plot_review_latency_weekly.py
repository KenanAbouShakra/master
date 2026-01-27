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
required = {"repo_full", "week", "review_latency_med_h"}
missing = required - set(df.columns)
if missing:
    raise ValueError(f"Missing columns in {path.name}: {sorted(missing)}")

df["week"] = pd.to_datetime(df["week"], utc=True, errors="coerce")
df = df.dropna(subset=["week", "review_latency_med_h", "repo_full"])
df = df.sort_values(["repo_full", "week"])

if df.empty:
    raise SystemExit("No valid rows after parsing review latency data.")

# -----------------------------
# 4-week rolling median smoothing
# -----------------------------
df["review_latency_smooth"] = (
    df.groupby("repo_full")["review_latency_med_h"]
      .transform(lambda s: s.rolling(4, min_periods=2).median())
)

# -----------------------------
# FIGURE: Faceted (one row per repo)
# -----------------------------
repos = sorted(df["repo_full"].unique())
fig, axes = plt.subplots(len(repos), 1, figsize=(11, 9), sharex=True)

# Handle single-repo case
if len(repos) == 1:
    axes = [axes]

for ax, repo in zip(axes, repos):
    sub = df[df["repo_full"] == repo]

    # Weekly median (lighter, noisy)
    ax.plot(
        sub["week"],
        sub["review_latency_med_h"],
        linewidth=1,
        alpha=0.3,
        label="Weekly median"
    )

    # Smoothed median (main signal)
    ax.plot(
        sub["week"],
        sub["review_latency_smooth"],
        linewidth=2.5,
        label="4-week median"
    )

    ax.set_title(repo)
    ax.set_ylabel("Hours")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left")

axes[-1].set_xlabel("Week")
fig.suptitle(
    "Review Latency Over Time (Median, Weekly and 4-week Smoothed)",
    y=1.02
)

plt.tight_layout()
out = FIG / "Figure_Review_Latency_Faceted_Weekly_4wMedian.png"
plt.savefig(out, dpi=300, bbox_inches="tight")
print("Saved:", out)
plt.close(fig)

print("Done.")