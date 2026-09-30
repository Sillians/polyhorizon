"""Read-only qualification of a saved, version-pinned dataset release."""
import argparse
import hashlib
import json
from pathlib import Path

from dotenv import load_dotenv

from polyhorizon.core.dataset_release import DatasetRelease
from polyhorizon.features.configs.settings import load_config
from polyhorizon.features.src.publish_dataset import read_release


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args()
    load_dotenv()
    release = DatasetRelease.model_validate_json(args.manifest.read_text())
    frame = read_release(release, load_config())
    canonical = json.dumps(release.model_dump(mode="json"), sort_keys=True).encode()
    print(json.dumps({"status": "passed", "dataset_id": str(release.dataset_id),
                      "manifest_sha256": hashlib.sha256(canonical).hexdigest(),
                      "feature_rows": len(frame), "evidence": release.session_qualification,
                      "published": False, "trained": False, "promoted": False}, indent=2))


if __name__ == "__main__":
    main()
