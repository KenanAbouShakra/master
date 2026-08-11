\
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

import yaml
from dateutil.parser import isoparse

from github_client import GitHubClient, GitHubConfig


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def make_client(cfg: Dict[str, Any]) -> GitHubClient:
    gh = cfg["github"]
    token = os.getenv(gh.get("token_env", "GITHUB_TOKEN"))
    if not token:
        raise RuntimeError(
            f"GitHub token missing. Export {gh.get('token_env', 'GITHUB_TOKEN')} before running bulk extraction."
        )
    storage = cfg["storage"]
    return GitHubClient(
        GitHubConfig(
            token=token,
            api_version=str(gh.get("api_version", "2026-03-10")),
            timeout_seconds=int(gh.get("timeout_seconds", 30)),
            max_retries=int(gh.get("max_retries", 6)),
            user_agent=str(gh.get("user_agent", "msc-ci-degradation-research/2.0")),
        ),
        cache_dir=Path(storage["cache_dir"]),
    )


def split_repo(repo_full: str) -> Tuple[str, str]:
    owner, repo = repo_full.split("/", 1)
    return owner, repo


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def dt(value: str | None):
    return isoparse(value) if value else None


def is_bot_user(user: Dict[str, Any] | None) -> bool:
    if not user:
        return False
    login = (user.get("login") or "").lower()
    typ = (user.get("type") or "").lower()
    return typ == "bot" or login.endswith("[bot]") or login.endswith("-bot")
