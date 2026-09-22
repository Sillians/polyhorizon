from __future__ import annotations

import os
from pathlib import Path

from polyhorizon.streaming.configs.settings import load_config
from polyhorizon.streaming.src.preflight_check import check_infrastructure
from polyhorizon.streaming.src.streaming_job import TradesStreamingJob


def main() -> None:
    config_path = os.getenv(
        "STREAMING_CONFIG_PATH",
        "/opt/app/polyhorizon/streaming/configs/spark_features.yaml",
    )

    config = load_config(config_path)

    if os.getenv("SKIP_PREFLIGHT", "false").lower() != "true":
        if not check_infrastructure(config):
            raise SystemExit("Pre-flight checks failed. Fix infra before running streaming job.")

    job = TradesStreamingJob(config=config)
    job.run(
        clean_start=os.getenv("CLEAN_START", "false").lower() == "true",
        dry_run=os.getenv("DRY_RUN", "false").lower() == "true",
    )
    output = os.getenv("DATASET_RELEASE_FILE")
    if output:
        if job.dataset_release is None:
            raise RuntimeError("Spark job exited without completing a dataset")
        Path(output).write_text(job.dataset_release.model_dump_json())


if __name__ == "__main__":
    main()
