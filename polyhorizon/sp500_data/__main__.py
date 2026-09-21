import argparse
from typing import Optional

from polyhorizon.sp500_data.configs.settings import load_config
from polyhorizon.sp500_data.get_sp500_companies import SP500Scraper


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and store S&P 500 constituents.")
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to sp500_variables.yaml (optional).",
    )
    return parser.parse_args()


def main(config_path: Optional[str] = None) -> None:
    config = load_config(config_path)
    SP500Scraper(config=config).run()


if __name__ == "__main__":
    args = parse_args()
    main(args.config)
