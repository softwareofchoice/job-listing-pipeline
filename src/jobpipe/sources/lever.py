from .base import Source


class Lever(Source):
    """Response: a bare JSON list of postings. Timestamps are epoch millis.

    An unknown company returns 404; a company with zero openings returns [].
    """

    name = "lever"

    def url(self, company_slug: str) -> str:
        return f"https://api.lever.co/v0/postings/{company_slug}?mode=json"

    def extract_postings(self, payload):
        return payload
