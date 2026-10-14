import json

import pytest

from streaming import openfda_producer as fda

FIXTURE = json.loads(open("tests/fixtures/openfda_enforcement.json").read())


class ListSink:
    def __init__(self):
        self.sent = []

    def send(self, key, record):
        self.sent.append((key, record))

    def close(self):
        pass


def test_normalise_shapes_record():
    rec = fda.normalise(FIXTURE["results"][0], 123)
    assert rec["recall_number"] == "D-0101-2026"
    assert rec["state"] is None  # empty strings become null
    assert rec["openfda_generic_name"] == ["EXAMPLICILLIN SODIUM"]
    assert rec["ingested_at"] == 123


def test_normalise_rejects_missing_key_and_fills_absent_openfda():
    assert fda.normalise(FIXTURE["results"][2], 1) is None
    rec = fda.normalise(FIXTURE["results"][1], 1)
    assert rec["openfda_brand_name"] == [] and rec["code_info"] is None


def test_run_skips_bad_and_duplicate_records():
    sink = ListSink()
    result = fda.run(FIXTURE["results"], sink)
    assert [k for k, _ in sink.sent] == ["D-0101-2026", "D-0102-2026"]
    assert result == {"sent": 2, "skipped": 2, "max_report_date": "20260218"}


class FakeResponse:
    def __init__(self, status, body):
        self.status_code, self._body = status, body

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


class FakeSession:
    """Serves `total` records in pages; records every call's params."""

    def __init__(self, total, page=fda.PAGE_SIZE):
        self.total, self.page, self.calls = total, page, []

    def get(self, url, params, timeout):
        self.calls.append(dict(params))
        start = params["skip"]
        n = max(0, min(self.page, self.total - start))
        results = [{"recall_number": f"D-{start + i:05d}-2026"} for i in range(n)]
        return FakeResponse(200, {"meta": {"results": {"total": self.total}}, "results": results})


def test_fetch_pages_paginates_until_total():
    s = FakeSession(total=2500)
    got = list(fda.fetch_pages("20260101", "20261014", session=s, pause_s=0))
    assert len(got) == 2500
    assert [c["skip"] for c in s.calls] == [0, 1000, 2000]
    assert s.calls[0]["search"] == "report_date:[20260101 TO 20261014]"


def test_fetch_pages_treats_404_as_no_results():
    class NotFound:
        def get(self, *a, **k):
            return FakeResponse(404, {})

    assert list(fda.fetch_pages("20260101", "20260102", session=NotFound(), pause_s=0)) == []


def test_fetch_pages_stops_at_skip_limit(caplog):
    s = FakeSession(total=40_000)
    got = list(fda.fetch_pages("20200101", "20261014", session=s, pause_s=0))
    assert len(got) == 26_000  # pages up to skip 25,000, then stops and warns
    assert "skip limit" in caplog.text


def test_state_roundtrip(tmp_path):
    p = tmp_path / "state.json"
    assert fda.load_state(p) is None
    fda.save_state(p, "20260218")
    assert fda.load_state(p) == "20260218"


@pytest.mark.parametrize("field", ["recall_number", "reason_for_recall", "report_date"])
def test_contract_string_fields_known(field):
    assert field in fda.STRING_FIELDS
