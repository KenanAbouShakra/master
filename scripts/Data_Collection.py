import os
import time
import requests
import pandas as pd

# -----------------------------
# Config
# -----------------------------
DAYS_BACK = 365
REPOS = [
    ("prometheus", "prometheus"),
    ("docker", "cli"),
]

GRAPHQL_URL = "https://api.github.com/graphql"
REST_URL = "https://api.github.com"

TOKEN = os.environ.get("GITHUB_TOKEN")
if not TOKEN:
    raise RuntimeError("Missing GITHUB_TOKEN env var. Set it before running.")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json",
}

# Collect only items newer than this
SINCE_ISO = (pd.Timestamp.utcnow() - pd.Timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%SZ")
print(f"Collecting data since: {SINCE_ISO} (last {DAYS_BACK} days)")

# -----------------------------
# GraphQL PR query (newest first)
# -----------------------------
PR_QUERY = """
query($owner:String!, $name:String!, $cursor:String) {
  repository(owner:$owner, name:$name) {
    pullRequests(
      first: 50,
      after: $cursor,
      orderBy: {field: CREATED_AT, direction: DESC},
      states: [OPEN, CLOSED, MERGED]
    ) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        createdAt
        mergedAt
        closedAt
        state
        isDraft
        additions
        deletions
        changedFiles
        commits { totalCount }
        author { login }
        mergeCommit { oid }
        reviews(first: 10) {
          nodes {
            createdAt
            state
            author { login }
          }
        }
      }
    }
  }
}
"""

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def safe_slug(owner: str, repo: str) -> str:
    return f"{owner}__{repo}".replace("/", "__")

def graphql_request(query: str, variables: dict) -> dict:
    """GraphQL request with timeout + retries"""
    for attempt in range(1, 6):
        try:
            r = requests.post(
                GRAPHQL_URL,
                headers=HEADERS,
                json={"query": query, "variables": variables},
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            if "errors" in data:
                raise RuntimeError(data["errors"])
            return data
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            wait = min(2 ** attempt, 30)
            print(f"[GraphQL] attempt {attempt}/5 failed ({type(e).__name__}). Retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError("GraphQL request failed after retries.")

def fetch_all_prs(owner: str, name: str) -> pd.DataFrame:
    rows = []
    cursor = None
    page = 0

    while True:
        page += 1
        data = graphql_request(PR_QUERY, {"owner": owner, "name": name, "cursor": cursor})
        pr_block = data["data"]["repository"]["pullRequests"]
        nodes = pr_block["nodes"] or []

        if not nodes:
            break

        print(f"[{owner}/{name}] PR page {page} fetched, rows so far: {len(rows)}")

        for pr in nodes:
            # Stop when we reach PRs older than SINCE_ISO
            if pr["createdAt"] < SINCE_ISO:
                print(f"[{owner}/{name}] reached PRs older than since-date; stopping PR collection.")
                return pd.DataFrame(rows)

            reviews = pr.get("reviews", {}).get("nodes", []) or []
            review_times = [rv["createdAt"] for rv in reviews if rv.get("createdAt")]
            first_review = min(review_times) if review_times else None

            rows.append({
                "owner": owner,
                "repo": name,
                "repo_full": f"{owner}/{name}",
                "pr_number": pr["number"],
                "created_at": pr["createdAt"],
                "merged_at": pr["mergedAt"],
                "closed_at": pr["closedAt"],
                "state": pr["state"],
                "is_draft": pr["isDraft"],
                "additions": pr["additions"],
                "deletions": pr["deletions"],
                "changed_files": pr["changedFiles"],
                "commit_count": pr["commits"]["totalCount"] if pr.get("commits") else None,
                "author": (pr["author"]["login"] if pr.get("author") else None),
                "merge_sha": (pr["mergeCommit"]["oid"] if pr.get("mergeCommit") else None),
                "first_review_at": first_review,
                "review_count": len(reviews),
            })

        if not pr_block["pageInfo"]["hasNextPage"]:
            break

        cursor = pr_block["pageInfo"]["endCursor"]
        time.sleep(0.2)

    return pd.DataFrame(rows)

def fetch_workflow_runs(owner: str, repo: str, max_pages: int = 20) -> pd.DataFrame:
    rows = []
    page = 1

    while page <= max_pages:
        url = f"{REST_URL}/repos/{owner}/{repo}/actions/runs"
        params = {"per_page": 100, "page": page}

        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        runs = data.get("workflow_runs", []) or []

        if not runs:
            break

        # Keep only runs within the time window (same as PRs)
        runs = [run for run in runs if run.get("created_at") and run["created_at"] >= SINCE_ISO]
        if not runs:
            print(f"[{owner}/{repo}] workflow page {page} has no runs within time window; stopping.")
            break

        print(f"[{owner}/{repo}] workflow page {page}, rows so far: {len(rows)}")

        for run in runs:
            prs = run.get("pull_requests", []) or []
            pr_numbers = [p.get("number") for p in prs if p.get("number")]

            rows.append({
                "owner": owner,
                "repo": repo,
                "repo_full": f"{owner}/{repo}",
                "run_id": run.get("id"),
                "name": run.get("name"),
                "event": run.get("event"),
                "status": run.get("status"),
                "conclusion": run.get("conclusion"),
                "created_at": run.get("created_at"),
                "run_started_at": run.get("run_started_at"),
                "updated_at": run.get("updated_at"),
                "head_sha": run.get("head_sha"),
                "pr_numbers": pr_numbers,
            })

        page += 1
        time.sleep(0.2)

    return pd.DataFrame(rows)

def to_dt(s):
    return pd.to_datetime(s, utc=True, errors="coerce")

def enrich_metrics(pr_df: pd.DataFrame, runs_df: pd.DataFrame):
    # ---- PR metrics ----
    pr_df = pr_df.copy()
    pr_df["created_at_dt"] = to_dt(pr_df["created_at"])
    pr_df["merged_at_dt"]  = to_dt(pr_df["merged_at"])
    pr_df["closed_at_dt"]  = to_dt(pr_df["closed_at"])
    pr_df["first_review_dt"] = to_dt(pr_df["first_review_at"])

    # done_at = merged_at hvis finnes, ellers closed_at
    pr_df["done_at_dt"] = pr_df["merged_at_dt"].fillna(pr_df["closed_at_dt"])
    pr_df["is_merged"] = pr_df["state"].eq("MERGED")

    pr_df["pr_cycle_hours"] = (pr_df["done_at_dt"] - pr_df["created_at_dt"]).dt.total_seconds() / 3600.0
    pr_df["review_latency_hours"] = (pr_df["first_review_dt"] - pr_df["created_at_dt"]).dt.total_seconds() / 3600.0
    pr_df["review_duration_hours"] = (pr_df["done_at_dt"] - pr_df["first_review_dt"]).dt.total_seconds() / 3600.0

    # ---- CI metrics ----
    runs_df = runs_df.copy()
    runs_df["run_started_dt"] = to_dt(runs_df["run_started_at"])
    runs_df["updated_dt"] = to_dt(runs_df["updated_at"])
    runs_df["ci_duration_min"] = (runs_df["updated_dt"] - runs_df["run_started_dt"]).dt.total_seconds() / 60.0
    runs_df["is_failure"] = runs_df["conclusion"].isin(["failure", "cancelled", "timed_out"])

    return pr_df, runs_df

def main():
    ensure_dir("data/raw")

    all_prs = []
    all_runs = []

    for owner, repo in REPOS:
        repo_full = f"{owner}/{repo}"
        slug = safe_slug(owner, repo)

        print(f"\n=== Processing {repo_full} ===")

        pr_df = fetch_all_prs(owner, repo)
        runs_df = fetch_workflow_runs(owner, repo, max_pages=20)

        pr_df, runs_df = enrich_metrics(pr_df, runs_df)

        print(f"[{repo_full}] PR rows: {len(pr_df)} | workflow rows: {len(runs_df)}")

        # Save per-repo files (nyttig for vedlegg og case-struktur)
        pr_repo_path = f"data/raw/prs__{slug}.csv"
        runs_repo_path = f"data/raw/workflow_runs__{slug}.csv"
        pr_df.to_csv(pr_repo_path, index=False)
        runs_df.to_csv(runs_repo_path, index=False)
        print(f"Saved per-repo:\n - {pr_repo_path}\n - {runs_repo_path}")

        all_prs.append(pr_df)
        all_runs.append(runs_df)

    # Save combined files (nyttig for samlet analyse og plotting)
    prs = pd.concat(all_prs, ignore_index=True) if all_prs else pd.DataFrame()
    runs = pd.concat(all_runs, ignore_index=True) if all_runs else pd.DataFrame()

    prs_path = "data/raw/prs.csv"
    runs_path = "data/raw/workflow_runs.csv"
    prs.to_csv(prs_path, index=False)
    runs.to_csv(runs_path, index=False)

    print("\nSaved combined:")
    print(" -", prs_path)
    print(" -", runs_path)

if __name__ == "__main__":
    main()
