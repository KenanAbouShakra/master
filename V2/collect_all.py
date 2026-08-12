from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import yaml


SCRIPT_DIR = Path(__file__).resolve().parent
PROGRESS_FILE = SCRIPT_DIR / "COLLECTION_PROGRESS.md"

COLLECTION_STEPS = (
    "extract_repositories.py",
    "extract_workflows.py",
    "extract_pull_requests.py",
    "extract_workflow_runs.py",
    "extract_attempts_jobs.py",
    "link_pr_ci.py",
    "export_core_tables.py",
)

ANALYSIS_STEPS = (
    "validate_dataset.py",
    "build_attempt_metrics.py",
)

STEPS = COLLECTION_STEPS + ANALYSIS_STEPS
GROUPS = ("pilot", "evaluation", "holdout", "reserve")

EXPORT_FILE_CANDIDATES = {
    "workflow_runs": ("workflow_runs.csv",),
    "prs": ("prs.csv", "pull_requests.csv"),
    "releases": ("releases.csv",),
    "jobs": ("workflow_jobs.csv", "jobs.csv"),
    "workflow_attempts": (
        "workflow_attempts.csv",
        "run_attempts.csv",
        "attempts.csv",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run collection, validation, and weekly panel construction."
    )
    parser.add_argument("--config", type=Path, default=Path("config.yaml"))
    parser.add_argument("--from-step", choices=STEPS)
    parser.add_argument("--to-step", choices=STEPS)
    parser.add_argument("--group", choices=GROUPS)
    parser.add_argument(
        "--reset-progress",
        action="store_true",
        help="Replace the existing progress table before recording this run.",
    )
    return parser.parse_args()


def load_config(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    with path.open(encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    if not isinstance(config, dict):
        raise ValueError("The configuration root must be a mapping")
    repositories = config.get("repositories")
    if not isinstance(repositories, list) or not repositories:
        raise ValueError("config.repositories must be a non-empty list")
    storage = config.get("storage")
    if not isinstance(storage, dict):
        raise ValueError("config.storage must be a mapping")
    for key in ("sqlite_path", "export_dir"):
        if not storage.get(key):
            raise ValueError(f"config.storage.{key} is required")
    return config


def selected_config(config: dict[str, Any], group: str | None) -> dict[str, Any]:
    result = dict(config)
    if group is None:
        result["repositories"] = list(config["repositories"])
        return result
    repository_groups = config.get("repository_groups")
    if not isinstance(repository_groups, dict) or group not in repository_groups:
        raise ValueError(f"Repository group not found in config: {group}")
    repositories = repository_groups[group]
    if not isinstance(repositories, list) or not repositories:
        raise ValueError(f"Repository group is empty or invalid: {group}")
    unknown = sorted(set(repositories) - set(config["repositories"]))
    if unknown:
        raise ValueError(
            f"Group {group} contains repositories absent from config.repositories: "
            + ", ".join(unknown)
        )
    result["repositories"] = list(repositories)
    return result


def resolve_config_paths(config: dict[str, Any], config_dir: Path) -> dict[str, Any]:
    result = dict(config)
    storage = dict(config["storage"])
    for key in ("sqlite_path", "export_dir"):
        path = Path(storage[key]).expanduser()
        if not path.is_absolute():
            path = (config_dir / path).resolve()
        storage[key] = str(path)
    result["storage"] = storage
    return result


def write_runtime_config(config: dict[str, Any], directory: Path) -> Path:
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".yaml",
        prefix="pipeline_",
        dir=directory,
        encoding="utf-8",
        delete=False,
    )
    try:
        yaml.safe_dump(config, handle, sort_keys=False, allow_unicode=True)
        return Path(handle.name)
    finally:
        handle.close()


def require_script(name: str) -> Path:
    path = SCRIPT_DIR / name
    if not path.is_file():
        raise FileNotFoundError(f"Pipeline script not found: {path}")
    return path


def find_export(export_dir: Path, key: str, required: bool) -> Path | None:
    candidates = [export_dir / name for name in EXPORT_FILE_CANDIDATES[key]]
    matches = [path for path in candidates if path.is_file()]
    if len(matches) > 1:
        names = ", ".join(str(path) for path in matches)
        raise RuntimeError(f"Ambiguous {key} exports; keep only one: {names}")
    if matches:
        return matches[0]
    if required:
        expected = ", ".join(str(path) for path in candidates)
        raise FileNotFoundError(f"Missing required {key} export; expected: {expected}")
    return None


def build_metrics_command(config: dict[str, Any]) -> list[str]:
    export_dir = Path(config["storage"]["export_dir"])
    command = [
        sys.executable,
        str(require_script("build_attempt_metrics.py")),
        "--workflow-runs",
        str(find_export(export_dir, "workflow_runs", required=True)),
    ]
    optional_arguments = (
        ("prs", "--prs"),
        ("releases", "--releases"),
        ("jobs", "--jobs"),
    )
    for key, argument in optional_arguments:
        path = find_export(export_dir, key, required=False)
        if path is not None:
            command.extend((argument, str(path)))
    attempts = find_export(export_dir, "workflow_attempts", required=True)
    command.extend(("--workflow-attempts", str(attempts)))
    command.extend(("--output-dir", str(export_dir)))
    return command


def run_step(name: str, config_path: Path, config: dict[str, Any]) -> None:
    print(f"\n=== {name} ===", flush=True)
    if name == "build_attempt_metrics.py":
        command = build_metrics_command(config)
    else:
        command = [
            sys.executable,
            str(require_script(name)),
            "--config",
            str(config_path),
        ]
    subprocess.run(command, check=True, cwd=SCRIPT_DIR)


def repository_group(config: dict[str, Any], repository: str) -> str:
    groups = config.get("repository_groups", {})
    if not isinstance(groups, dict):
        return "-"
    memberships = [name for name, repositories in groups.items() if repository in repositories]
    if len(memberships) > 1:
        raise ValueError(
            f"Repository belongs to multiple groups: {repository}: {', '.join(memberships)}"
        )
    return memberships[0] if memberships else "-"


def collect_stats(repository: str, config: dict[str, Any]) -> str:
    command = [
        sys.executable,
        str(require_script("repo_stats.py")),
        repository,
        "--db",
        str(config["storage"]["sqlite_path"]),
    ]
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        cwd=SCRIPT_DIR,
    )
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(f"repo_stats.py returned no output for {repository}")
    cells = [cell.strip() for cell in lines[-1].strip("|").split("|")]
    if cells and cells[0] == repository:
        cells = cells[1:]
    if len(cells) != 8:
        raise RuntimeError(
            f"Expected 8 statistic fields for {repository}, received {len(cells)}: "
            + " | ".join(cells)
        )
    group = repository_group(config, repository)
    return "| " + " | ".join((repository, group, *cells, "OK")) + " |"


def read_progress_rows(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    rows: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("|") or line.startswith("|---"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if cells and cells[0] != "Repository":
            rows[cells[0]] = line
    return rows


def write_progress(rows: dict[str, str]) -> None:
    header = (
        "# Collection Progress\n\n"
        "| Repository | Group | Workflows | PRs | Reviews | Commits | Runs | "
        "Attempts | Jobs | PR-CI Links | Status |\n"
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|\n"
    )
    body = "\n".join(rows[repository] for repository in sorted(rows))
    PROGRESS_FILE.write_text(header + body + "\n", encoding="utf-8")


def step_range(from_step: str | None, to_step: str | None) -> tuple[str, ...]:
    start = STEPS.index(from_step) if from_step else 0
    end = STEPS.index(to_step) if to_step else len(STEPS) - 1
    if start > end:
        raise ValueError("--from-step must not occur after --to-step")
    return STEPS[start : end + 1]


def main() -> int:
    args = parse_args()
    original_config_path = args.config.expanduser().resolve()
    original_config = load_config(original_config_path)
    config = selected_config(original_config, args.group)
    config = resolve_config_paths(config, original_config_path.parent)
    steps = step_range(args.from_step, args.to_step)
    runtime_config_path = write_runtime_config(config, original_config_path.parent)
    try:
        for step in steps:
            run_step(step, runtime_config_path, config)
        rows = {} if args.reset_progress else read_progress_rows(PROGRESS_FILE)
        for repository in config["repositories"]:
            rows[repository] = collect_stats(repository, config)
            print(f"[progress] {repository} saved to {PROGRESS_FILE.name}")
        write_progress(rows)
    finally:
        runtime_config_path.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)