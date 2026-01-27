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

# ============================================================
# FIGURE A
# ============================================================
for repo, sub in df.groupby("repo_full"):
    sub = sub.sort_values("week")

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(sub["week"], sub["share_with_retry"], label="Weekly", linewidth=1)
    ax.plot(sub["week"], sub["share_with_retry_smooth"], label="4-week avg", linewidth=2)

    ax.set_title(f"CI Flakiness Over Time (True Retry Proxy) — {repo}")
    ax.set_xlabel("Week")
    ax.set_ylabel("Share of workflow executions with retries")
    ax.grid(True, alpha=0.3)
    ax.legend()
    plt.tight_layout()

    out_repo = FIG_DIR / f"Figure_CI_Flakiness_{repo.replace('/','__')}.png"
    plt.savefig(out_repo, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print("Saved:", out_repo)

# ============================================================
# FIGURE B
# ============================================================
fig, ax = plt.subplots(figsize=(11, 5))
for repo, sub in df.groupby("repo_full"):
    sub = sub.sort_values("week")
    ax.plot(sub["week"], sub["share_with_retry_smooth"], label=f"{repo} (4w avg)", linewidth=2)

ax.set_title("CI Flakiness Over Time (True Retry Proxy) — 4-week averages only")
ax.set_xlabel("Week")
ax.set_ylabel("Share of workflow executions with retries")
ax.grid(True, alpha=0.3)
ax.legend()
plt.tight_layout()

out_avg = FIG_DIR / "Figure_CI_Flakiness_4wAvg_Only_Comparison.png"
plt.savefig(out_avg, dpi=300, bbox_inches="tight")
plt.close(fig)
print("Saved:", out_avg)

print("Done.")