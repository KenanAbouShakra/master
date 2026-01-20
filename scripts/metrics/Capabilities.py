from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIG_DIR = PROJECT_ROOT / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

metrics = ["PR Cycle Time", "Review Latency", "Merge Frequency", "CI Duration", "CI Failure Rate"]
caps = ["Fast Feedback", "Code Review", "Trunk-based Dev", "Continuous Integration", "Automated Testing"]

M = pd.DataFrame(
    [
        [1,0,0,0,0],
        [1,1,0,0,0],
        [0,0,1,0,0],
        [0,0,0,1,0],
        [0,0,0,1,1],
    ],
    index=metrics, columns=caps
)

fig, ax = plt.subplots(figsize=(10, 4))
ax.imshow(M.values)
ax.set_xticks(range(len(caps)))
ax.set_xticklabels(caps, rotation=30, ha="right")
ax.set_yticks(range(len(metrics)))
ax.set_yticklabels(metrics)

for i in range(M.shape[0]):
    for j in range(M.shape[1]):
        ax.text(j, i, str(M.iloc[i, j]), ha="center", va="center")

plt.title("Figure 2: Mapping Engineering Velocity Metrics to Accelerate Capabilities")
plt.tight_layout()

out = FIG_DIR / "Capabilities.png"
plt.savefig(out, dpi=300, bbox_inches="tight")
print("Saved:", out)

plt.show()
