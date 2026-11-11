from .base import Source


class Ashby(Source):
    """Response: {"jobs": [...], "apiVersion": "..."}.

    includeCompensation=true adds salary ranges when the company publishes them.
    """

    name = "ashby"

    def url(self, company_slug: str) -> str:
        return (
            f"https://api.ashbyhq.com/posting-api/job-board/{company_slug}"
            "?includeCompensation=true"
        )

    def extract_postings(self, payload):
        return payload["jobs"]
