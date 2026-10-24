"""Collect/qualify historical bars; publication is an explicit second action."""
import argparse
import json
from pathlib import Path

from dotenv import load_dotenv

from polyhorizon.ingestion.historical_backfill import historical_backfill_flow, save_json_exclusive


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="Inclusive NYSE date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="Inclusive completed NYSE date YYYY-MM-DD")
    parser.add_argument("--adjustment", required=True, choices=["unadjusted", "split_adjusted"])
    parser.add_argument("--csv", help="Optional provider export instead of Finnhub candles")
    parser.add_argument("--cache", default=".cache/historical")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--publish", action="store_true", help="Publish qualified TRAINING data only; never activate a model")
    args = parser.parse_args()
    load_dotenv()
    from polyhorizon.features.configs.settings import load_config
    config = load_config()
    root = config.bucket_details.feature_output_path
    manifest = historical_backfill_flow(args.start, args.end, args.adjustment, root,
                                        root.rstrip("/") + "/historical-bars", args.cache, args.csv)
    try:
        save_json_exclusive(args.manifest, manifest)
    except FileExistsError:
        if json.loads(args.manifest.read_text()) != manifest:
            raise ValueError("Output manifest exists with different evidence") from None
    result = {"dataset_id": manifest["dataset_id"], "manifest": str(args.manifest),
              "qualified": True, "published": False, "activated": False}
    if args.publish:
        from polyhorizon.features.src.publish_dataset import publish_dataset
        result["publication"] = publish_dataset(manifest, config=config)
        result["published"] = True
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
