from .base import Source


class Greenhouse(Source):
    """Response: {"jobs": [...], "meta": {"total": N}}.

    content=true includes the full (HTML-escaped) job description.
    """

    name = "greenhouse"

    def url(self, company_slug: str) -> str:
        return f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs?content=true"

    def extract_postings(self, payload):
        return payload["jobs"]
