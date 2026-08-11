import os
from datetime import datetime, timezone

import requests

API_URL = "https://api.github.com/repos/docker/cli/actions/runs"

TESTS = [
    ("OLD 2024-11-18", "2024-11-18T00:00:00Z..2024-11-18T23:59:59Z"),
    ("NEW 2026-06-01", "2026-06-01T00:00:00Z..2026-06-01T23:59:59Z"),
]

def main() -> None:
    token = os.environ.get("GITHUB_TOKEN")

    if not token:
        raise SystemExit("ERROR: GITHUB_TOKEN is not set")

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2026-03-10",
    }

    print(f"Test time (UTC): {datetime.now(timezone.utc).isoformat()}")
    print("Cache: not used")
    print()

    with requests.Session() as session:
        session.headers.update(headers)

        for label, created in TESTS:
            response = session.get(
                API_URL,
                params={
                    "created": created,
                    "per_page": 5,
                },
                timeout=30,
            )

            print(f"--- {label} ---")
            print(f"URL: {response.url}")
            print(f"HTTP: {response.status_code}")
            print(
                "X-RateLimit-Remaining:",
                response.headers.get("X-RateLimit-Remaining"),
            )

            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError:
                print("ERROR: Response was not valid JSON")
                print(response.text[:500])
                print()
                continue

            if not response.ok:
                print("ERROR:", data)
                print()
                continue

            runs = data.get("workflow_runs", [])

            print(f"total_count: {data.get('total_count')}")
            print(f"runs_returned: {len(runs)}")

            if runs:
                print(f"first_run_id: {runs[0].get('id')}")
                print(f"first_run_created: {runs[0].get('created_at')}")

            print()

if __name__ == "__main__":
    main()

