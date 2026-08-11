\
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Tuple

from dateutil.parser import isoparse

from db import connect, jdump, upsert
from utils import load_config, make_client, split_repo


def daterange_chunks(start: date, end: date, days: int = 14):
    cur = start
    while cur <= end:
        chunk_end = min(end, cur + timedelta(days=days - 1))
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def count_runs(client, owner, repo, start: date, end: date, event=None):
    params = {
        "created": f"{start.isoformat()}..{end.isoformat()}",
        "per_page": 1,
    }
    if event:
        params["event"] = event
    d = client.get_json(
        f"/repos/{owner}/{repo}/actions/runs",
        params=params,
        use_cache=False,
    )
    return int(d.get("total_count", 0))


def safe_windows(client, owner, repo, start: date, end: date, event=None, safety=900):
    # Splits date range until each window has fewer than `safety` runs (API caps at 1000).
    n = count_runs(client, owner, repo, start, end, event=event)
    if n <= safety:
        yield start, end, n
        return
    if start >= end:
        raise RuntimeError(
            f"{owner}/{repo} has {n} workflow runs on {start}; cannot avoid the 1000-search cap with date splitting."
        )
    midpoint = start + (end - start) // 2
    yield from safe_windows(client, owner, repo, start, midpoint, event, safety)
    yield from safe_windows(client, owner, repo, midpoint + timedelta(days=1), end, event, safety)


def run_row(repo_full, d):
    actor = d.get("actor") or {}
    trig = d.get("triggering_actor") or {}
    prs = d.get("pull_requests") or []
    return {
        "repo_full": repo_full,
        "run_id": d.get("id"),
        "workflow_id": d.get("workflow_id"),
        "name": d.get("name"),
        "display_title": d.get("display_title"),
        "path": d.get("path"),
        "run_number": d.get("run_number"),
        "current_run_attempt": d.get("run_attempt"),
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
        "actor_type": actor.get("type"),
        "triggering_actor_login": trig.get("login"),
        "pull_requests_json": jdump([
            {
                "number": p.get("number"),
                "url": p.get("url"),
                "head_sha": (p.get("head") or {}).get("sha"),
                "base_sha": (p.get("base") or {}).get("sha"),
            }
            for p in prs
        ]),
    }


def extract_repo_runs(client, con, repo_full, start_date, end_date, events):
    owner, repo = split_repo(repo_full)
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    total = 0

    event_filters = events if events else [None]

    for event in event_filters:
        for ws, we, expected in safe_windows(client, owner, repo, start, end, event=event):
            params = {
                "created": f"{ws.isoformat()}..{we.isoformat()}",
            }
            if event:
                params["event"] = event

            label = event or "all"
            print(f"[runs] {repo_full} event={label} {ws}..{we} expected={expected}")

            for d in client.paginate(
                f"/repos/{owner}/{repo}/actions/runs",
                params=params,
                item_key="workflow_runs",
                cache_prefix=f"{owner}_{repo}_runs_{label}_{ws}_{we}",
            ):
                upsert(con, "workflow_runs", run_row(repo_full, d), ["repo_full", "run_id"])
                total += 1
            con.commit()

    print(f"[runs] {repo_full}: total rows processed={total}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    cfg = load_config(args.config)
    client = make_client(cfg)
    con = connect(cfg["storage"]["sqlite_path"])
    events = cfg["study"].get("workflow_events") or []
    for repo_full in cfg["repositories"]:
        extract_repo_runs(
            client, con, repo_full,
            cfg["study"]["start_date"],
            cfg["study"]["end_date"],
            events,
        )


if __name__ == "__main__":
    main()
