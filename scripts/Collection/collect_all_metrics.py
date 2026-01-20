import os
import re
import time
import subprocess
from pathlib import Path
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd

# =============================
# CONFIG
# =============================
DAYS_BACK = 750
CHUNK_DAYS = 14

REPOS = [
    ("prometheus", "prometheus"),
    ("docker", "cli"),
    # You can add more:
    # ("argoproj", "argo-cd"),
    # ("grafana", "grafana"),
]

CD_WORKFLOW_NAME_PATTERNS = [r"deploy", r"release", r"publish", r"delivery", r"cd"]

# Sonar (optional)
SONAR_FREQUENCY = os.environ.get("SONAR_FREQUENCY", "monthly").lower()
SONAR_HOST_URL = os.environ.get("SONAR_HOST_URL")
SONAR_TOKEN = os.environ.get("SONAR_TOKEN")
SONAR_PROJECT_KEY_PREFIX = os.environ.get("SONAR_PROJECT_KEY_PREFIX", "mscthesis")

GRAPHQL_URL = "https://api.github.com/graphql"
REST_URL = "https://api.github.com"

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise RuntimeError("Missing GITHUB_TOKEN env var. Set it before running.")

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
}

SINCE_DT = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
SINCE_ISO = SINCE_DT.strftime("%Y-%m-%dT%H:%M:%SZ")

# =============================
# PATHS
# =============================
# scripts/collect_all_metrics.py -> project root is parent of scripts/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_DERIVED = PROJECT_ROOT / "data" / "derived"
REPO_CACHE = PROJECT_ROOT / "data" / "repos"
DATA_RAW.mkdir(parents=True, exist_ok=True)
DATA_DERIVED.mkdir(parents=True, exist_ok=True)
REPO_CACHE.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    print(msg, flush=True)

def safe_slug(owner: str, repo: str) -> str:
    return f"{owner}__{repo}"

def to_dt(s):
    return pd.to_datetime(s, utc=True, errors="coerce")

# =============================
# GraphQL PR Query (DESC)
# =============================
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
        reviews(first: 100) {
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

def graphql_request(query: str, variables: dict) -> dict:
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
            log(f"[GraphQL] attempt {attempt}/5 failed ({type(e).__name__}). retry {wait}s")
            time.sleep(wait)
    raise RuntimeError("GraphQL request failed after retries.")

def fetch_all_prs(owner: str, repo: str) -> pd.DataFrame:
    rows = []
    cursor = None
    page = 0

    while True:
        page += 1
        data = graphql_request(PR_QUERY, {"owner": owner, "name": repo, "cursor": cursor})
        pr_block = data["data"]["repository"]["pullRequests"]
        nodes = pr_block["nodes"] or []
        if not nodes:
            break

        log(f"[{owner}/{repo}] PR page {page} fetched. total rows: {len(rows)}")

        for pr in nodes:
            if pr["createdAt"] < SINCE_ISO:
                log(f"[{owner}/{repo}] reached PRs older than SINCE. stopping PRs.")
                return pd.DataFrame(rows)

            reviews = pr.get("reviews", {}).get("nodes", []) or []
            review_times = [rv["createdAt"] for rv in reviews if rv.get("createdAt")]
            first_review = min(review_times) if review_times else None

            rows.append({
                "owner": owner,
                "repo": repo,
                "repo_full": f"{owner}/{repo}",
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

# =============================
# Workflow runs in windows (fixes 2 months issue)
# =============================
def fetch_workflow_runs_by_windows(owner: str, repo: str, chunk_days: int = 14) -> pd.DataFrame:
    rows = []
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=DAYS_BACK)

    window_start = start
    while window_start < end:
        window_end = min(window_start + timedelta(days=chunk_days), end)
        created_param = f"{window_start.date()}..{window_end.date()}"
        log(f"[{owner}/{repo}] workflows window: {created_param}")

        page = 1
        while True:
            url = f"{REST_URL}/repos/{owner}/{repo}/actions/runs"
            params = {"per_page": 100, "page": page, "created": created_param}

            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            runs = data.get("workflow_runs", []) or []
            if not runs:
                break

            for run in runs:
                prs = run.get("pull_requests", []) or []
                pr_numbers = [p.get("number") for p in prs if p.get("number")]

                rows.append({
                    "owner": owner,
                    "repo": repo,
                    "repo_full": f"{owner}/{repo}",
                    "run_id": run.get("id"),
                    "workflow_name": run.get("name"),
                    "event": run.get("event"),
                    "status": run.get("status"),
                    "conclusion": run.get("conclusion"),
                    "created_at": run.get("created_at"),
                    "run_started_at": run.get("run_started_at"),
                    "updated_at": run.get("updated_at"),
                    "head_sha": run.get("head_sha"),
                    "pr_numbers": pr_numbers,
                })

            if len(runs) < 100:
                break
            page += 1
            time.sleep(0.2)

        window_start = window_end
        time.sleep(0.2)

    return pd.DataFrame(rows)

# =============================
# Releases (CD proxy)
# =============================
def fetch_releases(owner: str, repo: str, max_pages: int = 20) -> pd.DataFrame:
    rows = []
    page = 1
    while page <= max_pages:
        url = f"{REST_URL}/repos/{owner}/{repo}/releases"
        params = {"per_page": 100, "page": page}
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        rels = r.json() or []
        if not rels:
            break

        for rel in rels:
            published = rel.get("published_at") or rel.get("created_at")
            if published and published < SINCE_ISO:
                return pd.DataFrame(rows)

            rows.append({
                "owner": owner,
                "repo": repo,
                "repo_full": f"{owner}/{repo}",
                "release_id": rel.get("id"),
                "tag_name": rel.get("tag_name"),
                "name": rel.get("name"),
                "draft": rel.get("draft"),
                "prerelease": rel.get("prerelease"),
                "created_at": rel.get("created_at"),
                "published_at": rel.get("published_at"),
            })

        page += 1
        time.sleep(0.2)

    return pd.DataFrame(rows)

# =============================
# Enrich
# =============================
def enrich_prs(prs: pd.DataFrame) -> pd.DataFrame:
    prs = prs.copy()
    prs["created_at_dt"] = to_dt(prs["created_at"])
    prs["merged_at_dt"] = to_dt(prs["merged_at"])
    prs["closed_at_dt"] = to_dt(prs["closed_at"])
    prs["first_review_dt"] = to_dt(prs["first_review_at"])

    prs["done_at_dt"] = prs["merged_at_dt"].fillna(prs["closed_at_dt"])
    prs["is_merged"] = prs["state"].eq("MERGED")

    prs["pr_cycle_hours"] = (prs["done_at_dt"] - prs["created_at_dt"]).dt.total_seconds() / 3600.0
    prs["review_latency_hours"] = (prs["first_review_dt"] - prs["created_at_dt"]).dt.total_seconds() / 3600.0

    # ✅ Missing metric you asked for:
    # Review Duration = first review -> merge/done
    prs["review_duration_hours"] = (prs["done_at_dt"] - prs["first_review_dt"]).dt.total_seconds() / 3600.0

    # ✅ TD proxy: PR churn
    prs["pr_churn"] = prs["additions"].fillna(0) + prs["deletions"].fillna(0)

    return prs

def enrich_runs(runs: pd.DataFrame) -> pd.DataFrame:
    runs = runs.copy()
    runs["run_started_dt"] = to_dt(runs["run_started_at"])
    runs["updated_dt"] = to_dt(runs["updated_at"])
    runs["ci_duration_min"] = (runs["updated_dt"] - runs["run_started_dt"]).dt.total_seconds() / 60.0

    runs["is_failure"] = runs["conclusion"].isin(["failure", "cancelled", "timed_out"])

    pat = re.compile("|".join(CD_WORKFLOW_NAME_PATTERNS), re.IGNORECASE)
    runs["is_cd_workflow"] = runs["workflow_name"].fillna("").apply(lambda s: bool(pat.search(s)))

    return runs

def enrich_releases(rels: pd.DataFrame) -> pd.DataFrame:
    rels = rels.copy()
    rels["published_at_dt"] = to_dt(rels["published_at"])
    rels["created_at_dt"] = to_dt(rels["created_at"])
    rels["release_time_dt"] = rels["published_at_dt"].fillna(rels["created_at_dt"])
    return rels

# =============================
# Derived tables
# =============================
def derive_tables(prs: pd.DataFrame, runs: pd.DataFrame, rels: pd.DataFrame):
    prs = prs.copy()
    runs = runs.copy()
    rels = rels.copy()

    # Buckets
    prs["week"] = prs["created_at_dt"].dt.to_period("W").dt.start_time
    runs["week"] = runs["run_started_dt"].dt.to_period("W").dt.start_time

    # ✅ Merge frequency per week (you asked for this)
    merges_weekly = (
        prs[prs["is_merged"]].dropna(subset=["merged_at_dt"])
           .assign(week_merged=lambda d: d["merged_at_dt"].dt.to_period("W").dt.start_time)
           .groupby(["repo_full", "week_merged"], as_index=False)
           .agg(merge_frequency=("pr_number", "count"))
           .rename(columns={"week_merged": "week"})
           .sort_values("week")
    )

    # Review overhead weekly (includes review_latency + review_duration + review_count)
    review_weekly = (
        prs.dropna(subset=["week"])
           .groupby(["repo_full", "week"], as_index=False)
           .agg(
               pr_cycle_med_h=("pr_cycle_hours", "median"),
               review_latency_med_h=("review_latency_hours", "median"),
               review_duration_med_h=("review_duration_hours", "median"),
               review_count_med=("review_count", "median"),
               pr_churn_med=("pr_churn", "median"),
               merged_prs=("is_merged", "sum"),
               prs_total=("pr_number", "count"),
           )
           .sort_values("week")
    )

    # CI weekly
    ci_weekly = (
        runs.dropna(subset=["week", "ci_duration_min"])
            .groupby(["repo_full", "week"], as_index=False)
            .agg(
                ci_duration_med_min=("ci_duration_min", "median"),
                ci_failure_rate=("is_failure", "mean"),
                ci_runs=("run_id", "count"),
            )
            .sort_values("week")
    )

    # ✅ TD: CI flakiness = retry rate per SHA + volatility
    retry = (
        runs.dropna(subset=["week", "head_sha"])
            .groupby(["repo_full", "week", "head_sha"])
            .size()
            .reset_index(name="runs_per_sha")
    )
    flakiness_weekly = (
        retry.groupby(["repo_full", "week"], as_index=False)
             .agg(
                 avg_runs_per_sha=("runs_per_sha", "mean"),
                 p95_runs_per_sha=("runs_per_sha", lambda s: s.quantile(0.95)),
             )
             .sort_values("week")
    )

    # Failure volatility (std dev on weekly failure rate over rolling window)
    # (simple volatility proxy)
    ci_vol = ci_weekly.copy()
    ci_vol["failure_volatility_8w"] = (
        ci_vol.sort_values("week")
              .groupby("repo_full")["ci_failure_rate"]
              .rolling(8, min_periods=4)
              .std()
              .reset_index(level=0, drop=True)
    )

    # ✅ CD proxy: Release Frequency per month (you asked for this)
    if not rels.empty:
        rels["month"] = rels["release_time_dt"].dt.to_period("M").dt.start_time
        release_frequency_monthly = (
            rels.dropna(subset=["month"])
                .groupby(["repo_full", "month"], as_index=False)
                .agg(release_frequency=("release_id", "count"))
                .sort_values("month")
        )
    else:
        release_frequency_monthly = pd.DataFrame(columns=["repo_full", "month", "release_frequency"])

    # ✅ CD proxy: Release/Deploy workflow success rate (weekly)
    cd_runs = runs[runs["is_cd_workflow"]].copy()
    if not cd_runs.empty:
        cd_weekly = (
            cd_runs.dropna(subset=["week"])
                  .groupby(["repo_full", "week"], as_index=False)
                  .agg(
                      cd_runs=("run_id", "count"),
                      cd_failure_rate=("is_failure", "mean"),
                      cd_success_rate=("is_failure", lambda s: 1.0 - s.mean()),
                      cd_duration_med_min=("ci_duration_min", "median"),
                  )
                  .sort_values("week")
        )
    else:
        cd_weekly = pd.DataFrame(columns=["repo_full","week","cd_runs","cd_failure_rate","cd_success_rate","cd_duration_med_min"])

    # ✅ CD proxy: Time-to-Release (merge -> next release)
    if not rels.empty:
        rel_times = (rels[["repo_full", "release_time_dt"]]
                     .dropna()
                     .sort_values("release_time_dt"))

        ttr_rows = []
        for repo_full, pr_sub in prs[prs["is_merged"]].dropna(subset=["merged_at_dt"]).groupby("repo_full"):
            rel_sub = rel_times[rel_times["repo_full"] == repo_full]
            if rel_sub.empty:
                continue
            rel_list = rel_sub["release_time_dt"].tolist()

            for t in pr_sub["merged_at_dt"].tolist():
                # find first release >= merge time
                idx = next((i for i, rt in enumerate(rel_list) if rt >= t), None)
                if idx is not None:
                    ttr_days = (rel_list[idx] - t).total_seconds() / 86400.0
                    ttr_rows.append({"repo_full": repo_full, "merged_at_dt": t, "time_to_release_days": ttr_days})

        ttr = pd.DataFrame(ttr_rows)
        if not ttr.empty:
            ttr["month"] = ttr["merged_at_dt"].dt.to_period("M").dt.start_time
            time_to_release_monthly = (
                ttr.groupby(["repo_full","month"], as_index=False)
                   .agg(time_to_release_med_days=("time_to_release_days","median"),
                        n=("time_to_release_days","count"))
                   .sort_values("month")
            )
        else:
            time_to_release_monthly = pd.DataFrame(columns=["repo_full","month","time_to_release_med_days","n"])
    else:
        time_to_release_monthly = pd.DataFrame(columns=["repo_full","month","time_to_release_med_days","n"])

    return (
        review_weekly,
        ci_weekly,
        ci_vol,
        flakiness_weekly,
        merges_weekly,
        release_frequency_monthly,
        cd_weekly,
        time_to_release_monthly
    )

# =============================
# SonarQube snapshots (optional)
# =============================
def run_cmd(cmd, cwd=None):
    r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}"
        )
    return r.stdout.strip()

def ensure_repo_cloned(owner, repo):
    target = REPO_CACHE / safe_slug(owner, repo)
    if target.exists():
        return target
    url = f"https://github.com/{owner}/{repo}.git"
    log(f"Cloning {url} -> {target}")
    run_cmd(["git", "clone", url, str(target)])
    return target

def list_snapshot_dates(start_dt: datetime, end_dt: datetime, freq: str):
    dates = []
    cur = datetime(start_dt.year, start_dt.month, 1, tzinfo=timezone.utc)
    while cur < end_dt:
        dates.append(cur)
        if freq == "monthly":
            y = cur.year + (cur.month // 12)
            m = (cur.month % 12) + 1
            cur = datetime(y, m, 1, tzinfo=timezone.utc)
        elif freq == "quarterly":
            m = cur.month + 3
            y = cur.year + ((m - 1) // 12)
            m = ((m - 1) % 12) + 1
            cur = datetime(y, m, 1, tzinfo=timezone.utc)
        else:
            raise ValueError("SONAR_FREQUENCY must be 'monthly' or 'quarterly'")
    return dates

def git_checkout_commit_near_date(repo_path: Path, target_dt: datetime):
    # pick commit at or before date
    iso = target_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    commit = run_cmd(["git", "rev-list", "-n", "1", f"--before={iso}", "HEAD"], cwd=repo_path)
    if not commit:
        raise RuntimeError("No commit found before snapshot date.")
    run_cmd(["git", "checkout", commit], cwd=repo_path)
    return commit

def sonar_api_get(path: str, params: dict):
    if not SONAR_HOST_URL or not SONAR_TOKEN:
        raise RuntimeError("SONAR_HOST_URL and SONAR_TOKEN must be set for sonar snapshots.")
    url = f"{SONAR_HOST_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.get(url, params=params, auth=(SONAR_TOKEN, ""), timeout=60)
    r.raise_for_status()
    return r.json()

def run_sonar_snapshot(repo_path: Path, project_key: str):
    # Requires sonar-scanner installed
    run_cmd([
        "sonar-scanner",
        f"-Dsonar.projectKey={project_key}",
        f"-Dsonar.sources=.",
        f"-Dsonar.host.url={SONAR_HOST_URL}",
        f"-Dsonar.login={SONAR_TOKEN}",
    ], cwd=repo_path)

def collect_sonar_measures(project_key: str):
    keys = ",".join([
        "code_smells",
        "sqale_debt_ratio",
        "complexity",
        "duplicated_lines_density",
        "sqale_rating",
        "sqale_index",
    ])
    j = sonar_api_get("api/measures/component", {"component": project_key, "metricKeys": keys})
    measures = j.get("component", {}).get("measures", [])
    out = {m["metric"]: m.get("value") for m in measures}
    return out

def run_sonar_snapshots_for_repo(owner, repo):
    if not SONAR_HOST_URL or not SONAR_TOKEN:
        log("[Sonar] Skipping sonar snapshots (SONAR_HOST_URL/SONAR_TOKEN not set).")
        return pd.DataFrame()

    repo_path = ensure_repo_cloned(owner, repo)

    start = SINCE_DT
    end = datetime.now(timezone.utc)
    snaps = list_snapshot_dates(start, end, SONAR_FREQUENCY)

    rows = []
    project_key = f"{SONAR_PROJECT_KEY_PREFIX}:{owner}:{repo}"

    for snap_dt in snaps:
        log(f"[Sonar] {owner}/{repo} snapshot at {snap_dt.date()} ...")
        commit = git_checkout_commit_near_date(repo_path, snap_dt)

        run_sonar_snapshot(repo_path, project_key)
        measures = collect_sonar_measures(project_key)

        rows.append({
            "repo_full": f"{owner}/{repo}",
            "snapshot_date": snap_dt.isoformat(),
            "commit": commit,
            **measures
        })

    return pd.DataFrame(rows)

# =============================
# MAIN
# =============================
def main():
    log("=== collect_all_metrics.py START ===")
    log(f"Project root: {PROJECT_ROOT}")
    log(f"Data raw: {DATA_RAW}")
    log(f"Data derived: {DATA_DERIVED}")
    log(f"Collect since: {SINCE_ISO} (DAYS_BACK={DAYS_BACK})")
    log(f"Repos: {REPOS}")

    all_prs = []
    all_runs = []
    all_rels = []
    all_sonar = []

    for owner, repo in REPOS:
        repo_full = f"{owner}/{repo}"
        slug = safe_slug(owner, repo)

        log(f"\n=== Processing {repo_full} ===")

        prs = fetch_all_prs(owner, repo)
        runs = fetch_workflow_runs_by_windows(owner, repo, chunk_days=CHUNK_DAYS)
        rels = fetch_releases(owner, repo)

        log(f"[{repo_full}] raw PRs: {len(prs)} | raw runs: {len(runs)} | raw releases: {len(rels)}")

        if not prs.empty:
            prs = enrich_prs(prs)
        if not runs.empty:
            runs = enrich_runs(runs)
        if not rels.empty:
            rels = enrich_releases(rels)

        # Save raw per repo
        prs_path = DATA_RAW / f"prs__{slug}.csv"
        runs_path = DATA_RAW / f"workflow_runs__{slug}.csv"
        rels_path = DATA_RAW / f"releases__{slug}.csv"
        prs.to_csv(prs_path, index=False)
        runs.to_csv(runs_path, index=False)
        rels.to_csv(rels_path, index=False)
        log(f"Saved raw:\n - {prs_path}\n - {runs_path}\n - {rels_path}")

        all_prs.append(prs)
        all_runs.append(runs)
        all_rels.append(rels)

        # Sonar optional
        sonar_df = run_sonar_snapshots_for_repo(owner, repo)
        if not sonar_df.empty:
            sonar_path = DATA_RAW / f"sonar_snapshots__{slug}.csv"
            sonar_df.to_csv(sonar_path, index=False)
            log(f"[Sonar] Saved: {sonar_path}")
            all_sonar.append(sonar_df)

    # Combine raw
    prs_all = pd.concat(all_prs, ignore_index=True) if all_prs else pd.DataFrame()
    runs_all = pd.concat(all_runs, ignore_index=True) if all_runs else pd.DataFrame()
    rels_all = pd.concat(all_rels, ignore_index=True) if all_rels else pd.DataFrame()

    prs_all.to_csv(DATA_RAW / "prs.csv", index=False)
    runs_all.to_csv(DATA_RAW / "workflow_runs.csv", index=False)
    rels_all.to_csv(DATA_RAW / "releases.csv", index=False)
    log("\nSaved combined raw:\n - prs.csv\n - workflow_runs.csv\n - releases.csv")

    # Derived
    if not prs_all.empty and not runs_all.empty:
        (review_weekly, ci_weekly, ci_vol, flakiness_weekly,
         merges_weekly, release_freq_monthly, cd_weekly, ttr_monthly) = derive_tables(prs_all, runs_all, rels_all)

        review_weekly.to_csv(DATA_DERIVED / "review_overhead_weekly.csv", index=False)
        ci_weekly.to_csv(DATA_DERIVED / "ci_weekly.csv", index=False)
        ci_vol.to_csv(DATA_DERIVED / "ci_failure_volatility_weekly.csv", index=False)
        flakiness_weekly.to_csv(DATA_DERIVED / "ci_flakiness_weekly.csv", index=False)
        merges_weekly.to_csv(DATA_DERIVED / "merge_frequency_weekly.csv", index=False)
        release_freq_monthly.to_csv(DATA_DERIVED / "release_frequency_monthly.csv", index=False)
        cd_weekly.to_csv(DATA_DERIVED / "cd_workflow_weekly.csv", index=False)
        ttr_monthly.to_csv(DATA_DERIVED / "time_to_release_monthly.csv", index=False)

        log(f"\nSaved derived tables to: {DATA_DERIVED}")
    else:
        log("\nWARNING: Could not compute derived tables (empty PRs or runs).")

    # Combine sonar
    if all_sonar:
        sonar_all = pd.concat(all_sonar, ignore_index=True)
        sonar_all.to_csv(DATA_RAW / "sonar_snapshots.csv", index=False)
        log(f"[Sonar] Saved combined: {DATA_RAW / 'sonar_snapshots.csv'}")

    log("=== collect_all_metrics.py DONE ===")

if __name__ == "__main__":
    main()
