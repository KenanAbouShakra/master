\
from __future__ import annotations

import argparse
import sqlite3

import requests

from db import connect, jdump, upsert
from utils import load_config, make_client, split_repo


def attempt_row(repo_full, d, attempt_number):
    actor = d.get("actor") or {}
    trig = d.get("triggering_actor") or {}
    prs = d.get("pull_requests") or []
    return {
        "repo_full": repo_full,
        "run_id": d.get("id"),
        "attempt_number": attempt_number,
        "workflow_id": d.get("workflow_id"),
        "name": d.get("name"),
        "path": d.get("path"),
        "run_number": d.get("run_number"),
        "event": d.get("event"),
        "status": d.get("status"),
        "conclusion": d.get("conclusion"),
        "created_at": d.get("created_at"),
        "run_started_at": d.get("run_started_at"),
        "updated_at": d.get("updated_at"),
        "head_sha": d.get("head_sha"),
        "head_branch": d.get("head_branch"),
        "check_suite_id": d.get("check_suite_id"),
        "actor_login": actor.get("login"),
        "triggering_actor_login": trig.get("login"),
        "pull_requests_json": jdump([
            {
                "number": p.get("number"),
                "head_sha": (p.get("head") or {}).get("sha"),
                "base_sha": (p.get("base") or {}).get("sha"),
            }
            for p in prs
        ]),
    }


def job_row(repo_full, run_id, attempt_number, j):
    return {
        "repo_full": repo_full,
        "run_id": run_id,
        "attempt_number": attempt_number,
        "job_id": j.get("id"),
        "name": j.get("name"),
        "status": j.get("status"),
        "conclusion": j.get("conclusion"),
        "started_at": j.get("started_at"),
        "completed_at": j.get("completed_at"),
        "head_sha": j.get("head_sha"),
        "runner_id": j.get("runner_id"),
        "runner_name": j.get("runner_name"),
        "runner_group_id": j.get("runner_group_id"),
        "runner_group_name": j.get("runner_group_name"),
        "labels_json": jdump(j.get("labels") or []),
        "steps_json": jdump(j.get("steps") or []),
    }


def extract_run_attempts_jobs(client, con, repo_full, run_id, max_attempt):
    owner, repo = split_repo(repo_full)
    written_jobs = 0

    for attempt in range(1, max(1, int(max_attempt or 1)) + 1):
        try:
            d = client.get_json(
                f"/repos/{owner}/{repo}/actions/runs/{run_id}/attempts/{attempt}",
                cache_key=f"{owner}_{repo}_run_{run_id}_attempt_{attempt}",
            )
        except requests.HTTPError as e:
            
            if getattr(e.response, "status_code", None) == 404:
                print(f"[attempt missing] {repo_full} run={run_id} attempt={attempt}")
                continue
            raise

        upsert(
            con,
            "run_attempts",
            attempt_row(repo_full, d, attempt),
            ["repo_full", "run_id", "attempt_number"],
        )

        for j in client.paginate(
            f"/repos/{owner}/{repo}/actions/runs/{run_id}/attempts/{attempt}/jobs",
            item_key="jobs",
            cache_prefix=f"{owner}_{repo}_run_{run_id}_attempt_{attempt}_jobs",
        ):
            upsert(
                con,
                "workflow_jobs",
                job_row(repo_full, run_id, attempt, j),
                ["repo_full", "run_id", "attempt_number", "job_id"],
            )
            written_jobs += 1

        con.commit()
    return written_jobs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--repo", default=None, help="Optional owner/repo to process only one repository")
    ap.add_argument("--limit", type=int, default=None, help="Testing only: maximum runs")
    args = ap.parse_args()

    cfg = load_config(args.config)
    client = make_client(cfg)
    con = connect(cfg["storage"]["sqlite_path"])

    pr_events = {"pull_request", "pull_request_target", "merge_group"}

    repos = [args.repo] if args.repo else cfg["repositories"]
    for repo_full in repos:
        q = """
        SELECT run_id, current_run_attempt, event
        FROM workflow_runs
        WHERE repo_full=?
        ORDER BY created_at
        """
        params = [repo_full]
        rows = list(con.execute(q, params))
        if args.limit:
            rows = rows[: args.limit]

        pr_rows = [r for r in rows if (r["event"] or "") in pr_events]
        print(f"[attempts/jobs] {repo_full}: {len(pr_rows)}/{len(rows)} runs are PR-validation events")

        done = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT run_id FROM workflow_jobs WHERE repo_full=?", (repo_full,)
            )
        }

        for i, r in enumerate(pr_rows, 1):
            if r["run_id"] in done:
                continue
            jobs = extract_run_attempts_jobs(
                client, con, repo_full, r["run_id"], r["current_run_attempt"]
            )
            if i % 100 == 0:
                print(f"[attempts/jobs] {repo_full}: {i}/{len(rows)}, last jobs={jobs}")


if __name__ == "__main__":
    main()
