\
from __future__ import annotations

import argparse
from db import connect, upsert
from utils import load_config, make_client, split_repo, utcnow_iso


def extract_repo(client, con, repo_full: str):
    owner, repo = split_repo(repo_full)
    d = client.get_json(
        f"/repos/{owner}/{repo}",
        cache_key=f"{owner}_{repo}_repo",
    )
    row = {
        "repo_full": repo_full,
        "repo_id": d.get("id"),
        "owner": owner,
        "repo": repo,
        "default_branch": d.get("default_branch"),
        "archived": int(bool(d.get("archived"))),
        "fork": int(bool(d.get("fork"))),
        "created_at": d.get("created_at"),
        "updated_at": d.get("updated_at"),
        "pushed_at": d.get("pushed_at"),
        "stars": d.get("stargazers_count"),
        "forks_count": d.get("forks_count"),
        "open_issues_count": d.get("open_issues_count"),
        "language": d.get("language"),
        "extracted_at": utcnow_iso(),
    }
    upsert(con, "repositories", row, ["repo_full"])
    con.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    cfg = load_config(args.config)
    client = make_client(cfg)
    con = connect(cfg["storage"]["sqlite_path"])
    for repo_full in cfg["repositories"]:
        print(f"[repo] {repo_full}")
        extract_repo(client, con, repo_full)


if __name__ == "__main__":
    main()
