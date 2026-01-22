import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA = PROJECT_ROOT / "data" / "derived"
FIG = PROJECT_ROOT / "figures"
FIG.mkdir(exist_ok=True)

df = pd.read_csv(DATA / "pr_churn_pr_level.csv")

# Remove zero/negative churn (log-safety)
df = df[df["pr_churn"] > 0]

repos = []
values = []

for repo, sub in df.groupby("repo_full"):
    repos.append(repo)
    values.append(sub["pr_churn"])

fig, ax = plt.subplots(figsize=(8,5))
ax.boxplot(values, labels=repos, showfliers=True)

ax.set_yscale("log")
ax.set_ylabel("PR Churn (log scale)")
ax.set_title("PR Churn Distribution per Repository")

plt.tight_layout()
plt.savefig(FIG / "Figure_PR_Churn_Boxplot.png", dpi=300)
plt.show()
