from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Callable, Iterable, Optional, Protocol

import boto3
import pandas as pd
import requests
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from polyhorizon.core.logger import (
    get_logger,
    log_execution_time,
    log_shutdown,
    log_startup,
)
from polyhorizon.sp500_data.configs.settings import Config, load_config

logger = get_logger("S&P 500 Companies")

DEFAULT_TABLE_ID = "constituents"
SCHEMA_VERSION = "1.0"
SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,9}$")


class UniverseStorage(Protocol):
    def upload_file(
        self,
        data: bytes,
        filename: str,
        content_type: str,
        metadata: Optional[dict[str, str]] = None,
    ) -> None: ...

    def read_file(self, filename: str) -> Optional[bytes]: ...


class SeaweedStorage:
    """S3-compatible storage with errors propagated to the orchestrator."""

    def __init__(self, config: Config):
        self.bucket_name = config.bucket_details.sp500companies_bucket_name
        s3_config = BotoConfig(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "standard"},
        )
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=config.bucket_details.seaweed_s3_endpoint,
            aws_access_key_id=config.bucket_details.aws_access_key_id,
            aws_secret_access_key=config.bucket_details.aws_secret_access_key,
            region_name="us-east-1",
            config=s3_config,
        )
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except ClientError as exc:
            error_code = str(exc.response.get("Error", {}).get("Code", ""))
            if error_code not in {"404", "NoSuchBucket", "NotFound"}:
                raise
            logger.info("Creating universe bucket: %s", self.bucket_name)
            self.s3_client.create_bucket(Bucket=self.bucket_name)

    def upload_file(
        self,
        data: bytes,
        filename: str,
        content_type: str,
        metadata: Optional[dict[str, str]] = None,
    ) -> None:
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=filename,
            Body=data,
            ContentType=content_type,
            Metadata=metadata or {},
        )
        logger.info("Published s3://%s/%s", self.bucket_name, filename)

    def read_file(self, filename: str) -> Optional[bytes]:
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=filename)
        except ClientError as exc:
            error_code = str(exc.response.get("Error", {}).get("Code", ""))
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        return response["Body"].read()


@dataclass(frozen=True)
class Company:
    company_name: str
    symbol: str
    sector: Optional[str] = None
    sub_industry: Optional[str] = None
    cik: Optional[str] = None
    date_added: Optional[str] = None

    def to_dict(self) -> dict[str, Optional[str]]:
        return asdict(self)


@dataclass(frozen=True)
class UniversePolicy:
    minimum_constituents: int = 450
    maximum_constituents: int = 550
    maximum_change_fraction: float = 0.10
    request_timeout_seconds: int = 30


class UniverseValidationError(ValueError):
    """Raised when a candidate universe is unsafe to publish."""


class SP500Scraper:
    """Build and atomically activate a validated, point-in-time universe snapshot."""

    def __init__(
        self,
        config: Config,
        storage: Optional[UniverseStorage] = None,
        session: Optional[requests.Session] = None,
        now: Optional[Callable[[], datetime]] = None,
    ):
        self.config = config
        self.url = config.bucket_details.sp500_url
        self.storage = storage or SeaweedStorage(config=config)
        self.session = session or self._build_session()
        configured_policy = getattr(config, "universe_policy", None)
        self.policy = (
            UniversePolicy(**configured_policy.model_dump())
            if configured_policy is not None
            else UniversePolicy(minimum_constituents=1)
        )
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._source_payload: Optional[bytes] = None

    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {"User-Agent": "PolyHorizon-Universe/1.0 (+data-platform)"}
        )
        retries = Retry(
            total=4,
            connect=4,
            read=4,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def fetch_and_extract(self) -> list[Company]:
        """Fetch, parse, normalize, and validate a candidate universe."""
        logger.info("Fetching candidate universe from %s", self.url)
        response = self.session.get(
            self.url,
            timeout=self.policy.request_timeout_seconds,
        )
        response.raise_for_status()
        self._source_payload = response.content

        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table", {"id": DEFAULT_TABLE_ID})
        if table is None:
            raise UniverseValidationError(
                f"Required table #{DEFAULT_TABLE_ID} was not found"
            )

        header_row = table.find("thead")
        header_cells = (
            header_row.find_all("th") if header_row else table.find_all("th")
        )
        headers = [cell.get_text(" ", strip=True) for cell in header_cells]
        indexes = {
            "company_name": self._find_column_index(headers, ("security", "company")),
            "symbol": self._find_column_index(headers, ("symbol", "ticker")),
            "sector": self._find_column_index(
                headers,
                ("gics sector", "sector"),
                required=False,
            ),
            "sub_industry": self._find_column_index(
                headers,
                ("gics sub-industry", "sub-industry"),
                required=False,
            ),
            "cik": self._find_column_index(headers, ("cik",), required=False),
            "date_added": self._find_column_index(
                headers,
                ("date added",),
                required=False,
            ),
        }
        body = table.find("tbody")
        rows = body.find_all("tr") if body else table.find_all("tr")[1:]

        companies = []
        for row in rows:
            cells = row.find_all("td")
            if not cells:
                continue

            def value(field: str) -> Optional[str]:
                index = indexes[field]
                if index is None or index >= len(cells):
                    return None
                text = cells[index].get_text(" ", strip=True)
                return text or None

            company_name = value("company_name")
            raw_symbol = value("symbol")
            if company_name and raw_symbol:
                companies.append(
                    Company(
                        company_name=company_name,
                        symbol=self._normalize_symbol(raw_symbol),
                        sector=value("sector"),
                        sub_industry=value("sub_industry"),
                        cik=value("cik"),
                        date_added=value("date_added"),
                    )
                )

        candidate = sorted(self._deduplicate(companies), key=lambda item: item.symbol)
        self._validate_candidate(candidate)
        logger.info("Validated candidate universe with %d constituents", len(candidate))
        return candidate

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return symbol.strip().upper().replace("\u00a0", "")

    @staticmethod
    def _find_column_index(
        headers: list[str],
        candidates: Iterable[str],
        *,
        required: bool = True,
    ) -> Optional[int]:
        normalized = [header.casefold() for header in headers]
        for candidate in candidates:
            if candidate.casefold() in normalized:
                return normalized.index(candidate.casefold())
        if required:
            raise UniverseValidationError(
                f"Missing required source column; expected one of {list(candidates)}"
            )
        return None

    @staticmethod
    def _deduplicate(companies: list[Company]) -> list[Company]:
        by_symbol: dict[str, Company] = {}
        for company in companies:
            existing = by_symbol.get(company.symbol)
            if existing is not None and existing != company:
                raise UniverseValidationError(
                    f"Conflicting duplicate symbol: {company.symbol}"
                )
            by_symbol[company.symbol] = company
        return list(by_symbol.values())

    def _validate_candidate(self, companies: list[Company]) -> None:
        count = len(companies)
        if not self.policy.minimum_constituents <= count <= self.policy.maximum_constituents:
            raise UniverseValidationError(
                f"Constituent count {count} is outside "
                f"[{self.policy.minimum_constituents}, {self.policy.maximum_constituents}]"
            )
        invalid = [company.symbol for company in companies if not SYMBOL_PATTERN.fullmatch(company.symbol)]
        if invalid:
            raise UniverseValidationError(f"Invalid symbols: {invalid[:10]}")

    def _read_current_manifest(self) -> Optional[dict]:
        read_file = getattr(self.storage, "read_file", None)
        if read_file is None:
            return None
        payload = read_file("universe/current.json")
        return json.loads(payload) if payload else None

    def _validate_change(
        self,
        companies: list[Company],
        previous: Optional[dict],
    ) -> dict:
        current_symbols = {company.symbol for company in companies}
        previous_symbols = set((previous or {}).get("symbols", []))
        additions = sorted(current_symbols - previous_symbols)
        removals = sorted(previous_symbols - current_symbols)
        denominator = max(len(previous_symbols), 1)
        change_fraction = (len(additions) + len(removals)) / denominator
        if previous_symbols and change_fraction > self.policy.maximum_change_fraction:
            raise UniverseValidationError(
                f"Universe churn {change_fraction:.2%} exceeds "
                f"{self.policy.maximum_change_fraction:.2%}"
            )
        return {
            "additions": additions,
            "removals": removals,
            "change_fraction": change_fraction if previous_symbols else 0.0,
        }

    @staticmethod
    def _serialize(companies: list[Company]) -> dict[str, tuple[bytes, str]]:
        records = [company.to_dict() for company in companies]
        frame = pd.DataFrame(records)
        csv_bytes = frame.to_csv(index=False).encode("utf-8")
        json_bytes = json.dumps(
            records,
            indent=2,
            sort_keys=True,
        ).encode("utf-8")
        symbols_bytes = ("\n".join(company.symbol for company in companies) + "\n").encode()
        return {
            "companies.csv": (csv_bytes, "text/csv"),
            "companies.json": (json_bytes, "application/json"),
            "symbols.txt": (symbols_bytes, "text/plain"),
        }

    def save_data(self, companies: list[Company]) -> dict:
        """Publish immutable artifacts first and move the current pointer last."""
        self._validate_candidate(companies)
        previous = self._read_current_manifest()
        changes = self._validate_change(companies, previous)
        artifacts = self._serialize(companies)
        if self._source_payload is not None:
            artifacts["source.html"] = (self._source_payload, "text/html")
        fetched_at = self._now().astimezone(timezone.utc)
        run_id = f"{fetched_at:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:12]}"
        prefix = f"universe/snapshots/{fetched_at:%Y/%m/%d}/{run_id}"

        artifact_manifest = {}
        metadata = {
            "schema-version": SCHEMA_VERSION,
            "run-id": run_id,
        }
        for name, (payload, content_type) in artifacts.items():
            key = f"{prefix}/{name}"
            self.storage.upload_file(payload, key, content_type, metadata)
            artifact_manifest[name] = {
                "key": key,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "bytes": len(payload),
            }

        manifest = {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "source": self.url,
            "source_sha256": (
                hashlib.sha256(self._source_payload).hexdigest()
                if self._source_payload is not None
                else None
            ),
            "fetched_at": fetched_at.isoformat(),
            "constituent_count": len(companies),
            "symbols": [company.symbol for company in companies],
            "changes": changes,
            "artifacts": artifact_manifest,
        }
        manifest_bytes = json.dumps(
            manifest,
            indent=2,
            sort_keys=True,
        ).encode("utf-8")
        manifest_key = f"universe/manifests/{run_id}.json"
        self.storage.upload_file(
            manifest_bytes,
            manifest_key,
            "application/json",
            metadata,
        )

        # Compatibility objects are individually atomic S3 writes. The manifest
        # pointer remains the authoritative activation boundary for new consumers.
        aliases = {
            "sp500_companies.csv": artifacts["companies.csv"],
            "sp500_companies.json": artifacts["companies.json"],
            "sp500_companies.txt": artifacts["symbols.txt"],
        }
        for key, (payload, content_type) in aliases.items():
            self.storage.upload_file(payload, key, content_type, metadata)

        current = {**manifest, "manifest_key": manifest_key}
        self.storage.upload_file(
            json.dumps(current, indent=2, sort_keys=True).encode(),
            "universe/current.json",
            "application/json",
            metadata,
        )
        logger.info(
            "Activated universe %s: count=%d additions=%d removals=%d",
            run_id,
            len(companies),
            len(changes["additions"]),
            len(changes["removals"]),
        )
        return current

    @log_execution_time
    def run(self) -> dict:
        log_startup("S&P 500 Universe Service", self.config.project.version)
        try:
            companies = self.fetch_and_extract()
            return self.save_data(companies)
        finally:
            log_shutdown("S&P 500 Universe Service")


if __name__ == "__main__":
    SP500Scraper(config=load_config()).run()
