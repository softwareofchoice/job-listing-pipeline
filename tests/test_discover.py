import csv
import json

import pytest

from jobpipe.discover import (BraveSearch, build_queries, discover, existing_keys,
                              parse_board_url)


@pytest.mark.parametrize("url,expected", [
    ("https://boards.greenhouse.io/stripe/jobs/123", ("greenhouse", "stripe")),
    ("https://job-boards.greenhouse.io/figma/jobs/456?gh_jid=456", ("greenhouse", "figma")),
    ("https://boards.greenhouse.io/embed/job_app?for=acme&token=9", ("greenhouse", "acme")),
    ("https://jobs.lever.co/zoox/2a55d6c9-c564", ("lever", "zoox")),
    ("https://jobs.ashbyhq.com/ramp/d3bc1ced", ("ashby", "ramp")),
    ("https://JOBS.LEVER.CO/Zoox", ("lever", "Zoox")),          # host case-insensitive
    ("https://jobs.eu.lever.co/acme/1", None),                    # EU instance: different API
    ("https://job-boards.eu.greenhouse.io/acme/jobs/1", None),
    ("https://jobs.lever.co/", None),                             # no slug
    ("https://jobs.ashbyhq.com/Wander%20AI/1", None),             # unsafe slug
    ("https://www.linkedin.com/jobs/view/1", None),
])
def test_parse_board_url(url, expected):
    assert parse_board_url(url) == expected


def test_build_queries_covers_every_title_and_host():
    qs = build_queries(["Data Engineer", "Analytics Engineer"])
    assert len(qs) == 2 * 4  # 4 hosts: 2 greenhouse, 1 lever, 1 ashby
    assert qs[0].q == 'site:boards.greenhouse.io "Data Engineer"'


# ---------- fakes ----------

class Resp:
    def __init__(self, status, payload):
        self.status_code, self._payload = status, payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(str(self.status_code))


class FakeSession:
    """Routes Brave calls to `search_results[q]` (a list of pages) and
    job-board API calls to `boards[url-substring]`."""

    def __init__(self, search_results, boards):
        self.search_results, self.boards = search_results, boards
        self.brave_calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        if "api.search.brave.com" in url:
            self.brave_calls.append((params["q"], params["offset"]))
            pages = self.search_results.get(params["q"], [[]])
            urls = pages[params["offset"]] if params["offset"] < len(pages) else []
            more = params["offset"] + 1 < len(pages)
            return Resp(200, {"query": {"more_results_available": more},
                              "web": {"results": [{"url": u} for u in urls]}})
        for key, (status, payload) in self.boards.items():
            if key in url:
                return Resp(status, payload)
        return Resp(404, {})


@pytest.fixture
def companies(tmp_path):
    p = tmp_path / "companies.csv"
    p.write_text("company_slug,company_name,source,active,discovered_via,discovered_at\n"
                 "stripe,Stripe,greenhouse,true,manual,2026-11-11\n"
                 "oldco,OldCo,lever,false,manual,2026-11-11\n")
    return p


LEVER_DE = 'site:jobs.lever.co "Data Engineer"'
ASHBY_DE = 'site:jobs.ashbyhq.com "Data Engineer"'
GH_DE = 'site:boards.greenhouse.io "Data Engineer"'


def run(session, companies, pages=1, max_queries=100, dry_run=False):
    search = BraveSearch(session, "key", max_queries, pause_seconds=0)
    return discover(search, session, ["Data Engineer"], companies, pages, dry_run, pause_seconds=0)


def test_end_to_end_adds_only_new_valid_boards(companies):
    session = FakeSession(
        search_results={
            GH_DE: [["https://boards.greenhouse.io/STRIPE/jobs/1",      # known (case-insensitive)
                     "https://boards.greenhouse.io/newgh/jobs/2"]],
            LEVER_DE: [["https://jobs.lever.co/oldco/1",                 # inactive: still known
                        "https://jobs.lever.co/emptyco/1",               # 0 postings
                        "https://jobs.lever.co/deadco/1",                # 404
                        "https://jobs.ashbyhq.com/wrongsite/1"]],        # host mismatch: ignored
            ASHBY_DE: [["https://jobs.ashbyhq.com/newashby/1"]],
        },
        boards={
            "boards/newgh/": (200, {"jobs": [{"company_name": "New GH Inc"}], "meta": {}}),
            "postings/emptyco": (200, []),
            "job-board/newashby": (200, {"jobs": [{}, {}]}),
        },
    )
    m = run(session, companies)

    assert {(a["source"], a["company_slug"]) for a in m["added"]} == {
        ("greenhouse", "newgh"), ("ashby", "newashby")}
    assert {r["slug"]: r["reason"] for r in m["rejected"]} == {
        "deadco": "404 not found", "emptyco": "0 postings"}
    assert "wrongsite" not in json.dumps(m)

    rows = list(csv.DictReader(open(companies, newline="")))
    added = {r["company_slug"]: r for r in rows[2:]}
    assert added["newgh"]["company_name"] == "New GH Inc"       # from the API
    assert added["newashby"]["company_name"] == "Newashby"      # fallback from slug
    assert added["newgh"]["discovered_via"] == "brave:Data Engineer"
    assert m["queries_used"] == 4                               # one page per host


def test_dry_run_does_not_modify_csv(companies):
    before = companies.read_text()
    session = FakeSession({ASHBY_DE: [["https://jobs.ashbyhq.com/newashby/1"]]},
                          {"job-board/newashby": (200, {"jobs": [{}]})})
    m = run(session, companies, dry_run=True)
    assert m["n_added"] == 1 and companies.read_text() == before


def test_rerun_is_idempotent(companies):
    session = FakeSession({ASHBY_DE: [["https://jobs.ashbyhq.com/newashby/1"]]},
                          {"job-board/newashby": (200, {"jobs": [{}]})})
    run(session, companies)
    m = run(session, companies)
    assert m["n_added"] == 0
    assert len(existing_keys(companies)) == 3


def test_budget_cap_is_never_exceeded(companies):
    session = FakeSession({}, {})
    m = run(session, companies, pages=5, max_queries=3)
    assert len(session.brave_calls) == 3 and m["budget_exhausted"]


def test_pagination_stops_when_no_more_results(companies):
    session = FakeSession({LEVER_DE: [["https://jobs.lever.co/a/1"], ["https://jobs.lever.co/b/1"]]}, {})
    run(session, companies, pages=5)
    assert [o for q, o in session.brave_calls if q == LEVER_DE] == [0, 1]
