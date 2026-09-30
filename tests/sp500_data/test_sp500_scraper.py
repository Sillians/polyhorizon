import types
from datetime import datetime, timezone

import pytest

from polyhorizon.sp500_data.get_sp500_companies import (
    Company,
    SP500Scraper,
    UniverseValidationError,
)


class DummyResponse:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise ValueError("HTTP error")


class DummySession:
    def __init__(self, content: bytes):
        self._content = content
        self.headers = {}

    def get(self, url, timeout=None):
        return DummyResponse(self._content)

    def mount(self, *args, **kwargs):
        return None


class DummyStorage:
    def __init__(self, current=None):
        self.uploads = []
        self.current = current

    def upload_file(
        self,
        data: bytes,
        filename: str,
        content_type: str,
        metadata=None,
    ) -> None:
        self.uploads.append((filename, content_type, data, metadata))

    def read_file(self, filename: str):
        if filename == "universe/current.json" and self.current is not None:
            import json

            return json.dumps(self.current).encode()
        return None


def _fake_config():
    bucket_details = types.SimpleNamespace(
        sp500_url="http://example.com",
        sp500companies_bucket_name="sp500",
        seaweed_s3_endpoint="http://seaweed",
        aws_access_key_id="x",
        aws_secret_access_key="y",
    )
    project = types.SimpleNamespace(version="0.1.0")
    return types.SimpleNamespace(bucket_details=bucket_details, project=project)


def test_fetch_and_extract_parses_table():
    html = """
    <table id="constituents">
        <thead>
            <tr><th>Security</th><th>Symbol</th></tr>
        </thead>
        <tbody>
            <tr><td>Company A</td><td>AAA</td></tr>
            <tr><td>Company B</td><td>BBB</td></tr>
        </tbody>
    </table>
    """
    scraper = SP500Scraper(config=_fake_config(), storage=DummyStorage(), session=DummySession(html.encode("utf-8")))
    companies = scraper.fetch_and_extract()

    assert companies == [
        Company(company_name="Company A", symbol="AAA"),
        Company(company_name="Company B", symbol="BBB"),
    ]


def test_save_data_writes_three_formats():
    storage = DummyStorage()
    scraper = SP500Scraper(
        config=_fake_config(),
        storage=storage,
        session=DummySession(b""),
        now=lambda: datetime(2026, 7, 26, 6, 0, tzinfo=timezone.utc),
    )

    scraper.save_data(
        [
            Company(company_name="Company A", symbol="AAA"),
            Company(company_name="Company B", symbol="BBB"),
        ]
    )

    uploaded_names = [upload[0] for upload in storage.uploads]
    assert "sp500_companies.csv" in uploaded_names
    assert "sp500_companies.json" in uploaded_names
    assert "sp500_companies.txt" in uploaded_names
    assert "universe/current.json" == uploaded_names[-1]
    assert any(name.startswith("universe/snapshots/2026/07/26/") for name in uploaded_names)
    assert any(name.startswith("universe/manifests/") for name in uploaded_names)


def test_fetch_fails_closed_when_required_table_is_missing():
    scraper = SP500Scraper(
        config=_fake_config(),
        storage=DummyStorage(),
        session=DummySession(b"<html><body>no universe</body></html>"),
    )

    with pytest.raises(UniverseValidationError, match="Required table"):
        scraper.fetch_and_extract()


def test_rejects_suspicious_universe_churn_before_publication():
    storage = DummyStorage(
        current={"symbols": ["AAA", "BBB", "CCC", "DDD", "EEE"]}
    )
    config = _fake_config()
    config.universe_policy = types.SimpleNamespace(
        model_dump=lambda: {
            "minimum_constituents": 1,
            "maximum_constituents": 10,
            "maximum_change_fraction": 0.10,
            "request_timeout_seconds": 30,
        }
    )
    scraper = SP500Scraper(
        config=config,
        storage=storage,
        session=DummySession(b""),
    )

    with pytest.raises(UniverseValidationError, match="churn"):
        scraper.save_data([Company(company_name="Replacement", symbol="ZZZ")])

    assert storage.uploads == []
