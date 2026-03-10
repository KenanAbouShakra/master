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
df = df.dropna(subset=["week", "repo_full"]).sort_values(["repo_full", "week"])

# -----------------------------
# Optional: filter low-volume weeks
# -----------------------------
if "n_keys" in df.columns:
    MIN_KEYS = 20
    df = df[df["n_keys"] >= MIN_KEYS].copy()

# -----------------------------
# 4-week rolling mean per repo
# -----------------------------
df["share_with_retry_4w"] = (
    df.groupby("repo_full")["share_with_retry"]
      .transform(lambda s: s.rolling(window=4, min_periods=2).mean())
)

# -----------------------------
# Repo order
# -----------------------------
repo_order = [
    "docker/cli",
    "prometheus/prometheus",
    "tektoncd/pipeline",
]

available_repos = [r for r in repo_order if r in df["repo_full"].unique()]
if not available_repos:
    available_repos = list(df["repo_full"].unique())

# ============================================================
# FIGURE A: Faceted CI flakiness
# ============================================================
fig, axes = plt.subplots(len(available_repos), 1, figsize=(14, 4 * len(available_repos)), sharex=True)

if len(available_repos) == 1:
    axes = [axes]

for ax, repo in zip(axes, available_repos):
    sub = df[df["repo_full"] == repo].sort_values("week")

    ax.plot(sub["week"], sub["share_with_retry"], alpha=0.30, label="Weekly share")
    ax.plot(sub["week"], sub["share_with_retry_4w"], linewidth=2.5, label="4-week mean")

    ax.set_title(repo)
    ax.set_ylabel("Retry share")
    ax.grid(True, alpha=0.3)
    ax.legend()

axes[-1].set_xlabel("Week")
fig.suptitle("CI Flakiness Over Time (True Retry Proxy, Weekly and 4-week Smoothed)", fontsize=18, y=0.995)
fig.tight_layout()

out_faceted = FIG_DIR / "Figure_CI_Flakiness_Faceted_Weekly_4wMean.png"
fig.savefig(out_faceted, dpi=300, bbox_inches="tight")
plt.close(fig)
print("Saved:", out_faceted)

# ============================================================
# FIGURE B: Combined comparison (4-week smooth only)
# ============================================================
fig, ax = plt.subplots(figsize=(12, 5))

for repo in available_repos:
    sub = df[df["repo_full"] == repo].sort_values("week")
    ax.plot(sub["week"], sub["share_with_retry_4w"], linewidth=2.5, label=repo)

ax.set_title("CI Flakiness Over Time (True Retry Proxy, 4-week Smoothed)")
ax.set_xlabel("Week")
ax.set_ylabel("Retry share")
ax.grid(True, alpha=0.3)
ax.legend()
plt.tight_layout()

out_combined = FIG_DIR / "Figure_CI_Flakiness_4wMean_Comparison.png"
fig.savefig(out_combined, dpi=300, bbox_inches="tight")
plt.close(fig)
print("Saved:", out_combined)

print("Done.")