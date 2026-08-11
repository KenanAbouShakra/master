\
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


SCHEMA = r"""
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS repositories (
    repo_full TEXT PRIMARY KEY,
    repo_id INTEGER,
    owner TEXT,
    repo TEXT,
    default_branch TEXT,
    archived INTEGER,
    fork INTEGER,
    created_at TEXT,
    updated_at TEXT,
    pushed_at TEXT,
    stars INTEGER,
    forks_count INTEGER,
    open_issues_count INTEGER,
    language TEXT,
    extracted_at TEXT
);

CREATE TABLE IF NOT EXISTS workflows (
    repo_full TEXT NOT NULL,
    workflow_id INTEGER NOT NULL,
    name TEXT,
    path TEXT,
    state TEXT,
    created_at TEXT,
    updated_at TEXT,
    PRIMARY KEY (repo_full, workflow_id)
);

CREATE TABLE IF NOT EXISTS pull_requests (
    repo_full TEXT NOT NULL,
    pr_number INTEGER NOT NULL,
    node_id TEXT,
    state TEXT,
    draft INTEGER,
    created_at TEXT,
    updated_at TEXT,
    closed_at TEXT,
    merged_at TEXT,
    author_login TEXT,
    author_type TEXT,
    author_association TEXT,
    additions INTEGER,
    deletions INTEGER,
    changed_files INTEGER,
    commits_count INTEGER,
    comments_count INTEGER,
    review_comments_count INTEGER,
    head_sha TEXT,
    head_ref TEXT,
    head_repo_full TEXT,
    base_sha TEXT,
    base_ref TEXT,
    merge_commit_sha TEXT,
    PRIMARY KEY (repo_full, pr_number)
);

CREATE TABLE IF NOT EXISTS pr_commits (
    repo_full TEXT NOT NULL,
    pr_number INTEGER NOT NULL,
    sha TEXT NOT NULL,
    authored_at TEXT,
    committed_at TEXT,
    author_login TEXT,
    committer_login TEXT,
    PRIMARY KEY (repo_full, pr_number, sha)
);

CREATE TABLE IF NOT EXISTS pr_reviews (
    repo_full TEXT NOT NULL,
    pr_number INTEGER NOT NULL,
    review_id INTEGER NOT NULL,
    user_login TEXT,
    user_type TEXT,
    author_association TEXT,
    state TEXT,
    submitted_at TEXT,
    commit_id TEXT,
    body_present INTEGER,
    PRIMARY KEY (repo_full, review_id)
);

CREATE TABLE IF NOT EXISTS workflow_runs (
    repo_full TEXT NOT NULL,
    run_id INTEGER NOT NULL,
    workflow_id INTEGER,
    name TEXT,
    display_title TEXT,
    path TEXT,
    run_number INTEGER,
    current_run_attempt INTEGER,
    event TEXT,
    status TEXT,
    conclusion TEXT,
    created_at TEXT,
    run_started_at TEXT,
    updated_at TEXT,
    head_sha TEXT,
    head_branch TEXT,
    check_suite_id INTEGER,
    actor_login TEXT,
    actor_type TEXT,
    triggering_actor_login TEXT,
    pull_requests_json TEXT,
    PRIMARY KEY (repo_full, run_id)
);

CREATE TABLE IF NOT EXISTS run_attempts (
    repo_full TEXT NOT NULL,
    run_id INTEGER NOT NULL,
    attempt_number INTEGER NOT NULL,
    workflow_id INTEGER,
    name TEXT,
    path TEXT,
    run_number INTEGER,
    event TEXT,
    status TEXT,
    conclusion TEXT,
    created_at TEXT,
    run_started_at TEXT,
    updated_at TEXT,
    head_sha TEXT,
    head_branch TEXT,
    check_suite_id INTEGER,
    actor_login TEXT,
    triggering_actor_login TEXT,
    pull_requests_json TEXT,
    PRIMARY KEY (repo_full, run_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS workflow_jobs (
    repo_full TEXT NOT NULL,
    run_id INTEGER NOT NULL,
    attempt_number INTEGER NOT NULL,
    job_id INTEGER NOT NULL,
    name TEXT,
    status TEXT,
    conclusion TEXT,
    started_at TEXT,
    completed_at TEXT,
    head_sha TEXT,
    runner_id INTEGER,
    runner_name TEXT,
    runner_group_id INTEGER,
    runner_group_name TEXT,
    labels_json TEXT,
    steps_json TEXT,
    PRIMARY KEY (repo_full, run_id, attempt_number, job_id)
);

CREATE TABLE IF NOT EXISTS pr_ci_links (
    repo_full TEXT NOT NULL,
    pr_number INTEGER NOT NULL,
    run_id INTEGER NOT NULL,
    link_method TEXT NOT NULL,
    matched_sha TEXT,
    PRIMARY KEY (repo_full, pr_number, run_id, link_method)
);

CREATE TABLE IF NOT EXISTS extraction_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_full TEXT,
    component TEXT,
    started_at TEXT,
    completed_at TEXT,
    status TEXT,
    rows_written INTEGER,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_pr_commits_sha
ON pr_commits(repo_full, sha);

CREATE INDEX IF NOT EXISTS idx_runs_sha
ON workflow_runs(repo_full, head_sha);

CREATE INDEX IF NOT EXISTS idx_attempts_run
ON run_attempts(repo_full, run_id, attempt_number);

CREATE INDEX IF NOT EXISTS idx_jobs_run_attempt
ON workflow_jobs(repo_full, run_id, attempt_number);

CREATE INDEX IF NOT EXISTS idx_reviews_pr
ON pr_reviews(repo_full, pr_number);
"""


def connect(path: str | Path) -> sqlite3.Connection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    return con


def upsert(con: sqlite3.Connection, table: str, row: Dict[str, Any], pk_cols: Sequence[str]) -> None:
    cols = list(row)
    placeholders = ", ".join("?" for _ in cols)
    update_cols = [c for c in cols if c not in pk_cols]
    conflict = ", ".join(pk_cols)
    update_clause = ", ".join(f"{c}=excluded.{c}" for c in update_cols)
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT({conflict}) DO UPDATE SET {update_clause}"
    )
    con.execute(sql, [row[c] for c in cols])


def insert_ignore(con: sqlite3.Connection, table: str, row: Dict[str, Any]) -> None:
    cols = list(row)
    placeholders = ", ".join("?" for _ in cols)
    sql = f"INSERT OR IGNORE INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
    con.execute(sql, [row[c] for c in cols])


def jdump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
