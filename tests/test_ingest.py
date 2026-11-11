import json
from pathlib import Path

import pytest
import requests

from jobpipe.ingest import Company, fetch_one, load_companies
from jobpipe.sources import SOURCES
from jobpipe.storage import LocalRawStore


class FakeResponse:
    def __init__(self, status, body: bytes):
        self.status_code = status
        self.content = body

    def json(self):
        return json.loads(self.content)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.urls = []

    def get(self, url, timeout):
        self.urls.append(url)
        return self.response


@pytest.mark.parametrize("source,body,expected", [
    ("greenhouse", {"jobs": [{"id": 1}, {"id": 2}], "meta": {}}, 2),
    ("lever", [{"id": "a"}], 1),
    ("lever", [], 0),  # company exists but has no openings
    ("ashby", {"jobs": [{"id": "x"}] * 3, "apiVersion": "1"}, 3),
])
def test_each_source_counts_postings(source, body, expected):
    assert len(SOURCES[source].extract_postings(body)) == expected


def test_fetch_writes_raw_bytes_unchanged_and_metadata(tmp_path):
    body = b'{"jobs": [{"id": 1, "title": "Data Engineer"}], "meta": {}}'
    store = LocalRawStore(tmp_path)
    result = fetch_one(FakeSession(FakeResponse(200, body)), store,
                       Company("acme", "Acme", "greenhouse"), "2026-11-11", "run1")

    d = tmp_path / "source=greenhouse" / "dt=2026-11-11" / "company=acme"
    assert result["status"] == "ok"
    assert (d / "response.json").read_bytes() == body  # byte-for-byte raw
    meta = json.loads((d / "_metadata.json").read_text())
    assert meta["n_postings"] == 1 and meta["run_id"] == "run1"


def test_rerun_same_day_overwrites_not_duplicates(tmp_path):
    store = LocalRawStore(tmp_path)
    c = Company("acme", "Acme", "lever")
    fetch_one(FakeSession(FakeResponse(200, b'[{"id":"a"}]')), store, c, "2026-11-11", "r1")
    fetch_one(FakeSession(FakeResponse(200, b'[{"id":"a"},{"id":"b"}]')), store, c, "2026-11-11", "r2")

    files = sorted(p.name for p in tmp_path.rglob("*.json"))
    assert files == ["_metadata.json", "response.json"]
    assert json.loads((next(tmp_path.rglob("response.json"))).read_text())[1]["id"] == "b"


def test_http_error_is_recorded_and_nothing_written(tmp_path):
    store = LocalRawStore(tmp_path)
    result = fetch_one(FakeSession(FakeResponse(404, b'{"ok":false}')), store,
                       Company("nope", "Nope", "lever"), "2026-11-11", "r1")
    assert result["status"] == "failed" and result["http_status"] == 404
    assert not list(tmp_path.rglob("*.json"))


def test_non_json_response_is_rejected(tmp_path):
    result = fetch_one(FakeSession(FakeResponse(200, b"<html>maintenance</html>")),
                       LocalRawStore(tmp_path), Company("acme", "Acme", "ashby"), "2026-11-11", "r1")
    assert result["status"] == "failed"
    assert not list(tmp_path.rglob("*.json"))


def test_load_companies_skips_inactive_and_rejects_unknown_source(tmp_path):
    csv = tmp_path / "c.csv"
    csv.write_text("company_slug,company_name,source,active\n"
                   "a,A,lever,true\nb,B,ashby,false\n")
    assert [c.slug for c in load_companies(csv)] == ["a"]

    csv.write_text("company_slug,company_name,source,active\nz,Z,workday,true\n")
    with pytest.raises(ValueError):
        load_companies(csv)
