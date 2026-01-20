import matplotlib.pyplot as plt
from pathlib import Path

# --- paths ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIG_DIR = PROJECT_ROOT / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

fig, ax = plt.subplots(figsize=(12, 5))
ax.axis("off")

# --- boxes: updated pipeline ---
boxes = [
    ("Open-Source GitHub Repositories\n(prometheus/prometheus, docker/cli)", (0.05, 0.70)),
    ("Data Collection\nGitHub API\n• PRs + Reviews\n• Actions Workflow Runs\n• Releases (CD proxy)", (0.33, 0.70)),
    ("Metric Derivation (MSR)\nEngineering Velocity\n• Merge Frequency\n• Review Latency/Duration\nCI/CD Performance\n• CI Duration, Failure Rate\n• CD Workflow Success\n• Release Frequency, Time-to-Release", (0.62, 0.70)),
    ("Technical Debt Measurement\nProcess-based proxies\n• PR Churn\n• CI Flakiness (retry + volatility)\n• Review Overhead\nStatic analysis snapshots\n• SonarQube (monthly/quarterly)", (0.62, 0.28)),
    ("Analysis & Interpretation\n• Mapping to Accelerate Capabilities\n• Trend + cross-case comparison\n• Observability assessment", (0.90, 0.28)),
]

for text, (x, y) in boxes:
    ax.text(
        x, y, text,
        transform=ax.transAxes,
        ha="center", va="center",
        bbox=dict(boxstyle="round,pad=0.6"),
        fontsize=10
    )

# --- arrows (main flow) ---
ax.annotate("", xy=(0.25, 0.70), xytext=(0.14, 0.70),
            xycoords="axes fraction",
            arrowprops=dict(arrowstyle="->", lw=2))

ax.annotate("", xy=(0.52, 0.70), xytext=(0.41, 0.70),
            xycoords="axes fraction",
            arrowprops=dict(arrowstyle="->", lw=2))

# --- arrows (metrics -> TD -> analysis) ---
ax.annotate("", xy=(0.62, 0.42), xytext=(0.62, 0.58),
            xycoords="axes fraction",
            arrowprops=dict(arrowstyle="->", lw=2))

ax.annotate("", xy=(0.83, 0.28), xytext=(0.74, 0.28),
            xycoords="axes fraction",
            arrowprops=dict(arrowstyle="->", lw=2))

plt.title("Figure 1: Research Design Overview", fontsize=12)

# --- save ---
out = FIG_DIR / "Research_Design.png"
plt.savefig(out, dpi=300, bbox_inches="tight")
print("Saved:", out)

plt.show()
