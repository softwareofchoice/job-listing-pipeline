from urllib.parse import parse_qs, quote, urlparse

from .base import _SLUG_RE, Source


class Greenhouse(Source):
    """Response: {"jobs": [...], "meta": {"total": N}}.

    content=true includes the full (HTML-escaped) job description.
    Boards live on two hosts (old and new UI). EU-hosted boards
    (job-boards.eu.greenhouse.io) use a different API host, so they are
    intentionally not matched.
    """

    name = "greenhouse"
    board_hosts = ("boards.greenhouse.io", "job-boards.greenhouse.io")

    def url(self, company_slug: str) -> str:
        return f"https://boards-api.greenhouse.io/v1/boards/{quote(company_slug)}/jobs?content=true"

    def extract_postings(self, payload):
        return payload["jobs"]

    def company_name(self, payload):
        jobs = payload.get("jobs") or []
        return jobs[0].get("company_name") if jobs else None

    def slug_from_url(self, url):
        # Embedded boards: boards.greenhouse.io/embed/job_app?for=<slug>&token=...
        u = urlparse(url)
        if (u.hostname or "").lower() in self.board_hosts and u.path.startswith("/embed/"):
            slug = (parse_qs(u.query).get("for") or [""])[0]
            return slug if _SLUG_RE.match(slug) else None
        return super().slug_from_url(url)
