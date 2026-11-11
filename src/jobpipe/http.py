"""Polite HTTP client: identifies itself, times out, retries transient errors."""
from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USER_AGENT = "jobpipe/0.1 (personal job-search pipeline; one request per board per day)"


def make_session() -> requests.Session:
    retry = Retry(
        total=3,
        backoff_factor=2,  # waits 2s, 4s, 8s between attempts
        status_forcelist=(429, 500, 502, 503, 504),  # transient errors only
        allowed_methods=("GET",),
        respect_retry_after_header=True,  # honor a 429's Retry-After
    )
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session
