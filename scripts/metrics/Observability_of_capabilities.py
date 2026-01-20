from pathlib import Path
import matplotlib.pyplot as plt

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIG_DIR = PROJECT_ROOT / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

caps = [
    "Continuous Integration",
    "Trunk-based Development",
    "Fast Feedback",
    "Automated Testing",
    "Observability",
    "Lean Management",
    "Culture"
]

# Observability score on a 0–5 scale
scores = [5, 4, 5, 4, 2, 1, 0]

fig, ax = plt.subplots(figsize=(10, 4))
ax.bar(caps, scores)

ax.set_ylim(0, 5)
ax.set_ylabel("Observability Score (0–5)")
ax.set_title("Figure 6: Observability of Accelerate Capabilities via GitHub Data")

plt.xticks(rotation=30, ha="right")
plt.tight_layout()

out = FIG_DIR / "Observability_of_capabilities.png"
plt.savefig(out, dpi=300, bbox_inches="tight")
print("Saved:", out)

plt.show()
