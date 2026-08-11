\
from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional
from urllib.parse import urljoin

import requests


API_ROOT = "https://api.github.com/"


@dataclass
class GitHubConfig:
    token: Optional[str]
    api_version: str = "2026-03-10"
    timeout_seconds: int = 30
    max_retries: int = 6
    user_agent: str = "msc-ci-degradation-research/2.0"


class GitHubClient:
    """
    Minimal GitHub REST client with versioning, auth, pagination, retries, and caching.
    """

    def __init__(self, cfg: GitHubConfig, cache_dir: Optional[Path] = None):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": cfg.api_version,
            "User-Agent": cfg.user_agent,
        })
        if cfg.token:
            self.session.headers["Authorization"] = f"Bearer {cfg.token}"

        self.cache_dir = Path(cache_dir) if cache_dir else None
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self, key: str) -> Optional[Path]:
        if not self.cache_dir or not key:
            return None
        safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in key)
        return self.cache_dir / f"{safe}.json"

    def _wait_if_rate_limited(self, response: requests.Response) -> bool:
        if response.status_code not in (403, 429):
            return False

        remaining = response.headers.get("X-RateLimit-Remaining")
        reset = response.headers.get("X-RateLimit-Reset")
        retry_after = response.headers.get("Retry-After")

        # Secondary rate limits may not have remaining == 0.
        if retry_after:
            wait = max(1, int(retry_after))
            print(f"[rate-limit] Retry-After={wait}s")
            time.sleep(wait + random.uniform(0.5, 1.5))
            return True

        if remaining == "0" and reset:
            wait = max(1, int(reset) - int(time.time()) + 2)
            print(f"[rate-limit] primary limit exhausted; sleeping {wait}s")
            time.sleep(wait)
            return True

        # probable secondary limit
        if response.status_code == 403:
            wait = 60
            print(f"[rate-limit] probable secondary limit; sleeping {wait}s")
            time.sleep(wait + random.uniform(0, 5))
            return True
        return False

    def get_json(
        self,
        path_or_url: str,
        params: Optional[Dict[str, Any]] = None,
        cache_key: Optional[str] = None,
        use_cache: bool = True,
    ) -> Any:
        cache_path = self._cache_path(cache_key or "")
        if use_cache and cache_path and cache_path.exists():
            return json.loads(cache_path.read_text(encoding="utf-8"))

        url = path_or_url if path_or_url.startswith("http") else urljoin(API_ROOT, path_or_url.lstrip("/"))

        last_exc = None
        for attempt in range(self.cfg.max_retries):
            try:
                response = self.session.get(url, params=params, timeout=self.cfg.timeout_seconds)
                if self._wait_if_rate_limited(response):
                    continue

                if response.status_code == 404:
                    # A missing historical attempt/job is not transient.
                    response.raise_for_status()

                if 500 <= response.status_code < 600:
                    raise requests.HTTPError(
                        f"GitHub server error {response.status_code}: {response.url}",
                        response=response,
                    )

                response.raise_for_status()
                data = response.json()

                if cache_path:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    cache_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
                return data
            except requests.HTTPError as exc:
                # Do not retry permanent client errors such as 404/422.
                status = getattr(exc.response, "status_code", None)
                if status is not None and 400 <= status < 500 and status not in (403, 429):
                    raise
                last_exc = exc
                wait = min(60, 2 ** attempt) + random.uniform(0, 1)
                print(f"[retry {attempt + 1}/{self.cfg.max_retries}] {exc}; sleeping {wait:.1f}s")
                time.sleep(wait)
            except (requests.RequestException, ValueError) as exc:
                last_exc = exc
                wait = min(60, 2 ** attempt) + random.uniform(0, 1)
                print(f"[retry {attempt + 1}/{self.cfg.max_retries}] {exc}; sleeping {wait:.1f}s")
                time.sleep(wait)

        raise RuntimeError(f"GitHub request failed after retries: {url}") from last_exc

    def paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        item_key: Optional[str] = None,
        cache_prefix: Optional[str] = None,
    ) -> Iterator[Any]:
        params = dict(params or {})
        params["per_page"] = 100
        page = 1

        while True:
            page_params = dict(params)
            page_params["page"] = page
            cache_key = f"{cache_prefix}_p{page}" if cache_prefix else None
            data = self.get_json(path, page_params, cache_key=cache_key)

            if item_key is not None:
                items = data.get(item_key, [])
            else:
                items = data

            if not items:
                break

            for item in items:
                yield item

            if len(items) < 100:
                break
            page += 1

    def get_rate_limit(self) -> Dict[str, Any]:
        return self.get_json("/rate_limit", use_cache=False)
