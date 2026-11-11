"""Common interface for every job-board source (the "adapter" pattern).

Each provider has a different URL scheme and response shape. Instead of
sprinkling `if source == "lever"` checks around the code, each provider gets
one small class that knows:

  1. how to build the API URL for a company           (ingestion)
  2. how to find the postings inside its response      (ingestion)
  3. which public hostnames its job boards live on     (discovery)
  4. how to pull a company slug out of a board URL     (discovery)

Everything else (HTTP, retries, storage, search) is shared. Adding a 4th
provider means writing one new small class.
"""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Any
from urllib.parse import urlparse

# Slugs go into API URLs and storage paths, so only accept a safe character set.
_SLUG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class Source(ABC):
    name: str  # e.g. "greenhouse"; used as the storage partition key
    board_hosts: tuple[str, ...]  # public job-board hostnames, used for site: searches

    @abstractmethod
    def url(self, company_slug: str) -> str:
        """Public API endpoint for one company's job board."""

    @abstractmethod
    def extract_postings(self, payload: Any) -> list[dict]:
        """Return the list of postings inside a raw response.

        We do NOT transform the postings here. The raw layer stores the
        response untouched; this is only used to count/validate.
        """

    def company_name(self, payload: Any) -> str | None:
        """Display name from a response, if the provider includes one."""
        return None

    def slug_from_url(self, url: str) -> str | None:
        """Company slug from a public job-board URL, or None if not ours.

        Default rule: https://<board_host>/<slug>/... Override for odd cases.
        """
        u = urlparse(url)
        if (u.hostname or "").lower() not in self.board_hosts:
            return None
        first = u.path.strip("/").split("/", 1)[0]
        return first if _SLUG_RE.match(first) else None
