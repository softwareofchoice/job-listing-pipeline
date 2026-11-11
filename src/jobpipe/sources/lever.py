from urllib.parse import quote

from .base import Source


class Lever(Source):
    """Response: a bare JSON list of postings. Timestamps are epoch millis.

    An unknown company returns 404; a company with zero openings returns [].
    EU-hosted boards (jobs.eu.lever.co) use api.eu.lever.co, so they are
    intentionally not matched.
    """

    name = "lever"
    board_hosts = ("jobs.lever.co",)

    def url(self, company_slug: str) -> str:
        return f"https://api.lever.co/v0/postings/{quote(company_slug)}?mode=json"

    def extract_postings(self, payload):
        return payload
