\
from __future__ import annotations

import argparse

from dateutil.parser import isoparse

from db import connect, upsert
from utils import load_config, make_client, split_repo


def extract_pr_detail(client, owner, repo, number):
    return client.get_json(
        f"/repos/{owner}/{repo}/pulls/{number}",
        cache_key=f"{owner}_{repo}_pr_{number}",
    )


def extract_pr_commits(client, con, repo_full, owner, repo, pr_number):
    n = 0
    for c in client.paginate(
        f"/repos/{owner}/{repo}/pulls/{pr_number}/commits",
        cache_prefix=f"{owner}_{repo}_pr_{pr_number}_commits",
    ):
        commit = c.get("commit") or {}
        author = c.get("author") or {}
        committer = c.get("committer") or {}
        row = {
            "repo_full": repo_full,
            "pr_number": pr_number,
            "sha": c.get("sha"),
            "authored_at": (commit.get("author") or {}).get("date"),
            "committed_at": (commit.get("committer") or {}).get("date"),
            "author_login": author.get("login"),
            "committer_login": committer.get("login"),
        }
        upsert(con, "pr_commits", row, ["repo_full", "pr_number", "sha"])
        n += 1
    return n


def extract_pr_reviews(client, con, repo_full, owner, repo, pr_number):
    n = 0
    for r in client.paginate(
        f"/repos/{owner}/{repo}/pulls/{pr_number}/reviews",
        cache_prefix=f"{owner}_{repo}_pr_{pr_number}_reviews",
    ):
        user = r.get("user") or {}
        row = {
            "repo_full": repo_full,
            "pr_number": pr_number,
            "review_id": r.get("id"),
            "user_login": user.get("login"),
            "user_type": user.get("type"),
            "author_association": r.get("author_association"),
            "state": r.get("state"),
            "submitted_at": r.get("submitted_at"),
            "commit_id": r.get("commit_id"),
            "body_present": int(bool(r.get("body"))),
        }
        upsert(con, "pr_reviews", row, ["repo_full", "review_id"])
        n += 1
    return n


def extract_repo_prs(client, con, repo_full, start_date: str, end_date: str):
    owner, repo = split_repo(repo_full)
    start = isoparse(start_date + "T00:00:00Z")
    end = isoparse(end_date + "T23:59:59Z")
    written = 0

    done = {
        row[0]
        for row in con.execute(
            "SELECT pr_number FROM pull_requests WHERE repo_full=?", (repo_full,)
        )
    }

    for summary in client.paginate(
        f"/repos/{owner}/{repo}/pulls",
        params={"state": "all", "sort": "created", "direction": "desc"},
        cache_prefix=f"{owner}_{repo}_pulls_all_created_desc",
    ):
        created = isoparse(summary["created_at"])
        if created > end:
            continue
        if created < start:
            break

        number = summary["number"]
        if number in done:
            continue
        d = extract_pr_detail(client, owner, repo, number)
        user = d.get("user") or {}
        head = d.get("head") or {}
        base = d.get("base") or {}
        head_repo = head.get("repo") or {}

        row = {
            "repo_full": repo_full,
            "pr_number": number,
            "node_id": d.get("node_id"),
            "state": d.get("state"),
            "draft": int(bool(d.get("draft"))),
            "created_at": d.get("created_at"),
            "updated_at": d.get("updated_at"),
            "closed_at": d.get("closed_at"),
            "merged_at": d.get("merged_at"),
            "author_login": user.get("login"),
            "author_type": user.get("type"),
            "author_association": d.get("author_association"),
            "additions": d.get("additions"),
            "deletions": d.get("deletions"),
            "changed_files": d.get("changed_files"),
            "commits_count": d.get("commits"),
            "comments_count": d.get("comments"),
            "review_comments_count": d.get("review_comments"),
            "head_sha": head.get("sha"),
            "head_ref": head.get("ref"),
            "head_repo_full": head_repo.get("full_name"),
            "base_sha": base.get("sha"),
            "base_ref": base.get("ref"),
            "merge_commit_sha": d.get("merge_commit_sha"),
        }
        upsert(con, "pull_requests", row, ["repo_full", "pr_number"])

        extract_pr_commits(client, con, repo_full, owner, repo, number)
        extract_pr_reviews(client, con, repo_full, owner, repo, number)
        con.commit()
        written += 1
        if written % 50 == 0:
            print(f"[prs] {repo_full}: {written}")

    print(f"[prs] {repo_full}: total {written}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    cfg = load_config(args.config)
    client = make_client(cfg)
    con = connect(cfg["storage"]["sqlite_path"])
    for repo_full in cfg["repositories"]:
        extract_repo_prs(
            client, con, repo_full,
            cfg["study"]["start_date"],
            cfg["study"]["end_date"],
        )


if __name__ == "__main__":
    main()
