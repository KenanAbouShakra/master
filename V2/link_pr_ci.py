\
from __future__ import annotations

import argparse
import json

from db import connect, insert_ignore
from utils import load_config


def link_by_commit_sha(con, repo_full):
    # Links a workflow run to a PR when run.head_sha matches a PR commit SHA.
    sql = """
    SELECT DISTINCT pc.pr_number, wr.run_id, wr.head_sha
    FROM pr_commits pc
    JOIN workflow_runs wr
      ON wr.repo_full = pc.repo_full
     AND wr.head_sha = pc.sha
    WHERE pc.repo_full = ?
    """
    n = 0
    for r in con.execute(sql, [repo_full]):
        insert_ignore(con, "pr_ci_links", {
            "repo_full": repo_full,
            "pr_number": r["pr_number"],
            "run_id": r["run_id"],
            "link_method": "commit_sha",
            "matched_sha": r["head_sha"],
        })
        n += 1
    con.commit()
    return n


def link_direct_pr_array(con, repo_full):
    n = 0
    for r in con.execute(
        "SELECT run_id, head_sha, pull_requests_json FROM workflow_runs WHERE repo_full=?",
        [repo_full],
    ):
        try:
            prs = json.loads(r["pull_requests_json"] or "[]")
        except Exception:
            prs = []
        for p in prs:
            number = p.get("number")
            if not number:
                continue
            insert_ignore(con, "pr_ci_links", {
                "repo_full": repo_full,
                "pr_number": int(number),
                "run_id": r["run_id"],
                "link_method": "api_pull_requests_array",
                "matched_sha": p.get("head_sha") or r["head_sha"],
            })
            n += 1
    con.commit()
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    cfg = load_config(args.config)
    con = connect(cfg["storage"]["sqlite_path"])

    for repo_full in cfg["repositories"]:
        a = link_by_commit_sha(con, repo_full)
        b = link_direct_pr_array(con, repo_full)
        print(f"[links] {repo_full}: commit_sha={a}, direct={b}")


if __name__ == "__main__":
    main()
