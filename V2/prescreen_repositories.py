\
from __future__ import annotations

import argparse
from datetime import date, timedelta
from dateutil.parser import isoparse

from utils import load_config, make_client, split_repo


def count_recent_closed_prs(client, owner, repo, start_date):
    # Low-cost approximation for pre-screen:
    # paginate pulls newest-first and count closed/merged PRs created in window.
    n = 0
    for p in client.paginate(
        f"/repos/{owner}/{repo}/pulls",
        params={"state": "closed", "sort": "created", "direction": "desc"},
        cache_prefix=f"prescreen_{owner}_{repo}_closed_prs",
    ):
        created = isoparse(p["created_at"]).date()
        if created < start_date:
            break
        n += 1
    return n


def count_actions_runs(client, owner, repo, start_date, end_date):
    # total_count over an entire filtered window can be capped for retrieval,
    # but it is useful as a coarse pre-screen signal only.
    d = client.get_json(
        f"/repos/{owner}/{repo}/actions/runs",
        params={"created": f"{start_date.isoformat()}..{end_date.isoformat()}", "per_page": 1},
        use_cache=False,
    )
    return int(d.get("total_count", 0))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("repos", nargs="*", help="Optional owner/repo list; defaults to config repositories")
    args = ap.parse_args()

    cfg = load_config(args.config)
    client = make_client(cfg)
    repos = args.repos or cfg["repositories"]

    end = date.fromisoformat(cfg["study"]["end_date"])
    min_days = int(cfg["prescreen"].get("min_history_days", 730))
    history_start = end - timedelta(days=min_days)

    print("repo_full\tarchived\tfork\tpushed_at\tclosed_prs_window\tactions_runs_window\teligible_basic")
    for repo_full in repos:
        owner, repo = split_repo(repo_full)
        meta = client.get_json(f"/repos/{owner}/{repo}", cache_key=f"prescreen_{owner}_{repo}_meta")
        prs = count_recent_closed_prs(client, owner, repo, history_start)
        runs = count_actions_runs(client, owner, repo, history_start, end)

        eligible = (
            (not meta.get("archived"))
            and (not meta.get("fork"))
            and prs >= int(cfg["prescreen"].get("min_closed_prs", 500))
            and runs >= int(cfg["prescreen"].get("min_actions_runs", 500))
        )
        print(
            f"{repo_full}\t{meta.get('archived')}\t{meta.get('fork')}\t"
            f"{meta.get('pushed_at')}\t{prs}\t{runs}\t{eligible}"
        )


if __name__ == "__main__":
    main()
