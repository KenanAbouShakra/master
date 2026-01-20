import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DERIVED = PROJECT_ROOT / "data" / "derived"
FIG_DIR = PROJECT_ROOT / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

in_path = DERIVED / "ci_flakiness_true_retry_weekly.csv"
if not in_path.exists():
    raise FileNotFoundError(f"Missing {in_path}. Run the true-retry flakiness script first.")

df = pd.read_csv(in_path)

# -----------------------------
# Parse time
# -----------------------------
df["week"] = pd.to_datetime(df["week"], errors="coerce")
df = df.dropna(subset=["week", "repo_full"]).sort_values("week")

# -----------------------------
# Optional: filter low-volume weeks
# -----------------------------
# If you have n_keys, you can filter to reduce noise:
if "n_keys" in df.columns:
    MIN_KEYS = 20
    df = df[df["n_keys"] >= MIN_KEYS].copy()

# -----------------------------
# Smooth: 4-week rolling mean per repo
# -----------------------------
df["share_with_retry_smooth"] = (
    df.groupby("repo_full")["share_with_retry"]
      .transform(lambda s: s.rolling(4, min_periods=2).mean())
)

# -----------------------------
# FIGURE 1: Share of commits with retries (weekly)
# -----------------------------
fig, ax = plt.subplots(figsize=(11, 5))
for repo, sub in df.groupby("repo_full"):
    sub = sub.sort_values("week")
    ax.plot(sub["week"], sub["share_with_retry"], label=f"{repo} (weekly)", linewidth=1)
    ax.plot(sub["week"], sub["share_with_retry_smooth"], label=f"{repo} (4w avg)", linewidth=2)

ax.set_title("CI Flakiness Over Time (True Retry Proxy)")
ax.set_xlabel("Week")
ax.set_ylabel("Share of workflow executions with retries")
ax.grid(True, alpha=0.3)
ax.legend()
plt.tight_layout()

out1 = FIG_DIR / "Figure_CI_Flakiness_Share_With_Retry_Weekly.png"
plt.savefig(out1, dpi=300, bbox_inches="tight")
print("Saved:", out1)
plt.close(fig)

# -----------------------------
# FIGURE 2: Boxplot of avg retry intensity by repo
# -----------------------------
# avg_runs_per_key ~ 1 means usually no retry; >1 means retries common
if "avg_runs_per_key" in df.columns:
    fig, ax = plt.subplots(figsize=(8, 5))

    repos = []
    data = []
    for repo, sub in df.groupby("repo_full"):
        repos.append(repo)
        data.append(sub["avg_runs_per_key"].dropna().values)

    ax.boxplot(data, labels=repos, showfliers=True)
    ax.set_title("Distribution of CI Retry Intensity (Avg runs per workflow+event+SHA)")
    ax.set_ylabel("Avg runs per key (1 = no retries)")
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()

    out2 = FIG_DIR / "Figure_CI_Flakiness_Retry_Intensity_Boxplot.png"
    plt.savefig(out2, dpi=300, bbox_inches="tight")
    print("Saved:", out2)
    plt.close(fig)
else:
    print("Skipped boxplot: 'avg_runs_per_key' not found in input CSV.")

print("Done.")
