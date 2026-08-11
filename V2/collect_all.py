\
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import yaml


STEPS = [
    "extract_repositories.py",
    "extract_workflows.py",
    "extract_pull_requests.py",
    "extract_workflow_runs.py",
    "extract_attempts_jobs.py",
    "link_pr_ci.py",
    "build_attempt_metrics.py",
    "validate_dataset.py",
]

PROGRESS_FILE = Path(__file__).parent / "COLLECTION_PROGRESS.md"


def save_stats(repo: str, config_path: str) -> None:
    result = subprocess.run(
        [sys.executable, str(Path(__file__).parent / "repo_stats.py"), repo, "--db",
         yaml.safe_load(open(config_path))["storage"]["sqlite_path"]],
        capture_output=True, text=True,
    )
    line = result.stdout.strip()
    if not line:
        line = f"| {repo} | - | - | - | - | - | - | - | - |"

    if not PROGRESS_FILE.exists():
        PROGRESS_FILE.write_text(
            "# Collection Progress\n\n"
            "| Repository | Workflows | PRs | Reviews | Commits | Runs | Attempts | Jobs | PR-CI Links | Status |\n"
            "|---|---|---|---|---|---|---|---|---|---|\n"
        )
    with PROGRESS_FILE.open("a") as f:
        f.write(line.rstrip("|").rstrip() + " OK |\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--from-step", default=None, choices=STEPS)
    ap.add_argument("--to-step", default=None, choices=STEPS)
    ap.add_argument("--group", default=None,
                    choices=["pilot", "evaluation", "holdout", "reserve"],
                    help="Run only repos from a named group (requires repository_groups in config)")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config))

    if args.group:
        groups = cfg.get("repository_groups", {})
        if args.group not in groups:
            print(f"Group '{args.group}' not found in config.")
            sys.exit(1)
        cfg["repositories"] = groups[args.group]
    start = STEPS.index(args.from_step) if args.from_step else 0
    end = STEPS.index(args.to_step) if args.to_step else len(STEPS) - 1

    for script in STEPS[start : end + 1]:
        print(f"\n=== {script} ===")
        subprocess.run(
            [sys.executable, str(Path(__file__).parent / script), "--config", args.config],
            check=True,
        )

    for repo in cfg["repositories"]:
        save_stats(repo, args.config)
        print(f"[progress] {repo} saved to {PROGRESS_FILE.name}")


if __name__ == "__main__":
    main()
