from __future__ import annotations

import requests

import renomear_ebooks as re


class _FakeResponse:
    def __init__(self, status_code: int = 200, content_type: str = "application/json", payload=None):
        self.status_code = status_code
        self.headers = {"Content-Type": content_type}
        self._payload = payload if payload is not None else {}
        self.text = "{}"

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            err = requests.exceptions.HTTPError(f"http {self.status_code}")
            err.response = self
            raise err

    def json(self):
        if self._payload == "__invalid_json__":
            raise ValueError("bad json")
        return self._payload


class _FakeSession:
    def __init__(self, response_or_exc):
        self.response_or_exc = response_or_exc

    def get(self, *args, **kwargs):  # noqa: ANN002, ANN003
        if isinstance(self.response_or_exc, Exception):
            raise self.response_or_exc
        return self.response_or_exc


def test_get_json_timeout_records_failure(monkeypatch) -> None:
    monkeypatch.setattr(re, "_get_http_session", lambda: _FakeSession(requests.exceptions.Timeout("t")))
    fails: list[dict[str, str]] = []
    out = re.get_json("https://x", None, {}, 0.0, source="openlibrary", source_failures=fails)
    assert isinstance(out, dict) and out.get("_error") == "timeout"
    assert fails and fails[0]["source"] == "openlibrary"


def test_get_json_http_500_records_failure(monkeypatch) -> None:
    monkeypatch.setattr(re, "_get_http_session", lambda: _FakeSession(_FakeResponse(status_code=500)))
    fails: list[dict[str, str]] = []
    out = re.get_json("https://x", None, {}, 0.0, source="google", source_failures=fails)
    assert isinstance(out, dict) and "http_500" in str(out.get("_error", ""))
    assert fails and "http_500" in fails[0]["reason"]


def test_get_json_invalid_json_records_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        re,
        "_get_http_session",
        lambda: _FakeSession(_FakeResponse(status_code=200, payload="__invalid_json__")),
    )
    fails: list[dict[str, str]] = []
    out = re.get_json("https://x", None, {}, 0.0, source="wikipedia", source_failures=fails)
    assert isinstance(out, dict) and "invalid_json" in str(out.get("_error", ""))
    assert fails and "invalid_json" in fails[0]["reason"]


def test_source_timeout_does_not_block_other_sources(monkeypatch) -> None:
    def _ol_fail(*args, **kwargs):  # noqa: ANN002, ANN003
        raise requests.exceptions.Timeout("openlibrary down")

    def _gb_ok(*args, **kwargs):  # noqa: ANN002, ANN003
        meta = args[0]
        return re.BookMeta(meta.path, title=meta.title, authors=["Autor"], year="2001", source="googlebooks")

    monkeypatch.setattr(re, "best_openlibrary", _ol_fail)
    monkeypatch.setattr(re, "best_googlebooks", _gb_ok)

    local = re.BookMeta("a.pdf", title="Livro", authors=["X"], year="")
    merged = re.lookup_metadata(local, frozenset({"openlibrary", "google"}), {}, 0.0, False)
    assert merged.year == "2001"
    assert any(f.get("source") == "openlibrary" for f in merged.source_failures)


def test_all_external_sources_fail_marks_low_confidence_review_band(monkeypatch) -> None:
    def _fail(*args, **kwargs):  # noqa: ANN002, ANN003
        raise requests.exceptions.ConnectionError("offline")

    monkeypatch.setattr(re, "best_openlibrary", _fail)
    monkeypatch.setattr(re, "best_googlebooks", _fail)
    monkeypatch.setattr(re, "best_skoob_year", _fail)
    monkeypatch.setattr(re, "best_book_catalogs_ddgs_year", _fail)
    monkeypatch.setattr(re, "best_wikipedia", _fail)
    monkeypatch.setattr(re, "best_web_year", _fail)

    local = re.BookMeta("a.pdf", title="Livro", authors=["Autor"], year="")
    merged = re.lookup_metadata(local, re.ALL_REMOTE_SOURCES, {}, 0.0, False)
    score, _ = re.compute_match_evidence(local, merged)
    assert merged.source_failures
    assert re._review_band(score) != "auto"


def test_failure_on_one_candidate_does_not_stop_next(monkeypatch) -> None:
    state = {"n": 0}

    def _ol_flaky(*args, **kwargs):  # noqa: ANN002, ANN003
        state["n"] += 1
        if state["n"] == 1:
            raise requests.exceptions.Timeout("first fails")
        meta = args[0]
        return re.BookMeta(meta.path, title=meta.title, authors=meta.authors, year="1999", source="openlibrary")

    monkeypatch.setattr(re, "best_openlibrary", _ol_flaky)
    monkeypatch.setattr(re, "best_googlebooks", lambda *a, **k: None)

    one = re.lookup_metadata(
        re.BookMeta("1.pdf", title="A", authors=["AA"], year=""),
        frozenset({"openlibrary"}),
        {},
        0.0,
        False,
    )
    two = re.lookup_metadata(
        re.BookMeta("2.pdf", title="B", authors=["BB"], year=""),
        frozenset({"openlibrary"}),
        {},
        0.0,
        False,
    )

    assert one.path.endswith("1.pdf")
    assert two.year == "1999"
