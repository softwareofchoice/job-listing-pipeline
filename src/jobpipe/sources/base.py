"""Common interface for every job-board source (the "adapter" pattern).

Each provider has a different URL and response shape. Instead of sprinkling
`if source == "lever"` checks around the code, each provider gets one small
class that knows two things:

  1. how to build the URL for a company
  2. how to count the postings in its response (for logging/validation)

Everything else (HTTP, retries, storage) is shared. Adding a 4th provider
(e.g. Workable) means writing one new ~15-line class.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Source(ABC):
    name: str  # e.g. "greenhouse"; used as the storage partition key

    @abstractmethod
    def url(self, company_slug: str) -> str:
        """Public API endpoint for one company's job board."""

    @abstractmethod
    def extract_postings(self, payload: Any) -> list[dict]:
        """Return the list of postings inside a raw response.

        We do NOT transform the postings here. The raw layer stores the
        response untouched; this is only used to count/validate.
        """
