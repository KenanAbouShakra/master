\
from __future__ import annotations

import argparse

from db import connect, upsert
from utils import load_config, make_client, split_repo


def extract_workflows(client, con, repo_full: str):
    owner, repo = split_repo(repo_full)
    count = 0
    for d in client.paginate(
        f"/repos/{owner}/{repo}/actions/workflows",
        item_key="workflows",
        cache_prefix=f"{owner}_{repo}_workflows",
    ):
        row = {
            "repo_full": repo_full,
            "workflow_id": d.get("id"),
            "name": d.get("name"),
            "path": d.get("path"),
            "state": d.get("state"),
            "created_at": d.get("created_at"),
            "updated_at": d.get("updated_at"),
        }
        upsert(con, "workflows", row, ["repo_full", "workflow_id"])
        count += 1
    con.commit()
    print(f"[workflows] {repo_full}: {count}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    cfg = load_config(args.config)
    client = make_client(cfg)
    con = connect(cfg["storage"]["sqlite_path"])
    for repo_full in cfg["repositories"]:
        extract_workflows(client, con, repo_full)


if __name__ == "__main__":
    main()
