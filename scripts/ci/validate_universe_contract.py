"""Validate the governed S&P 500 universe architecture."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def require(text: str, marker: str, message: str) -> None:
    if marker not in text:
        raise SystemExit(message)


def main() -> None:
    publisher = (
        ROOT / "polyhorizon/sp500_data/get_sp500_companies.py"
    ).read_text()
    reader = (ROOT / "polyhorizon/sp500_data/reader.py").read_text()
    flow = (ROOT / "polyhorizon/sp500_data/flow.py").read_text()
    prefect = (ROOT / "prefect.yaml").read_text()
    ingestion = (
        ROOT / "polyhorizon/ingestion/finnhub_producer/producer.py"
    ).read_text()
    features = (ROOT / "polyhorizon/features/feast_ops/entity_df.py").read_text()

    for marker in (
        "minimum_constituents",
        "maximum_change_fraction",
        "universe/snapshots/",
        "universe/manifests/",
        "sha256",
        '"universe/current.json"',
    ):
        require(publisher, marker, f"Universe publisher is missing {marker}")
    if publisher.rfind('"universe/current.json"') < publisher.find("manifest_key"):
        raise SystemExit("The current pointer must be published after immutable artifacts")

    for marker in (
        "schema_version",
        "maximum_age_hours",
        "checksum",
        "constituent_count",
        "duplicate symbols",
    ):
        require(reader, marker, f"Universe reader is missing {marker}")

    require(flow, "retries=3", "Universe publication task must retry")
    require(
        prefect,
        "name: sp500-universe-weekdays",
        "Prefect must schedule the universe refresh",
    )
    require(
        prefect,
        'cron: "0 7 * * 1-5"',
        "Universe refresh must run before market ingestion",
    )
    for consumer, name in ((ingestion, "ingestion"), (features, "features")):
        require(
            consumer,
            "load_governed_universe",
            f"{name} must consume the governed universe pointer",
        )

    print("S&P 500 universe contract is consistent.")


if __name__ == "__main__":
    main()
