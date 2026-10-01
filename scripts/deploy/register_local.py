"""Register local Docker deployments, paused by default; execute in Prefect image.

PREFECT_API_URL must target the local server. No secrets are stored in job variables.
"""
import argparse
from pathlib import Path


def specifications(env_file, network, low_memory=False):
    common = {"PREFECT_API_URL": "http://prefect-server:4200/api", "PREFECT_API_KEY": "",
              "PREFECT_HOME": "/tmp/prefect",
              "SPARK_MASTER_URL": "spark://spark-master:7077",
              "SEAWEED_S3_ENDPOINT": "http://seaweedfs-s3:8333", "POSTGRES_HOST": "postgres",
              "POSTGRES_PORT": "5432", "REDIS_HOST": "redis", "REDIS_PORT": "6379",
              "KAFKA_SERVERS": "kafka-broker-1:29092,kafka-broker-2:29093,kafka-broker-3:29094",
              "KAFKA_BOOTSTRAP_SERVERS": "kafka-broker-1:29092,kafka-broker-2:29093,kafka-broker-3:29094",
              "PUSHGATEWAY_URL": "http://prometheus-pushgateway:9091",
              "FEATURES_PUSHGATEWAY_URL": "http://prometheus-pushgateway:9091",
              "UNIVERSE_CURRENT_KEY": "universe/current.json", "UNIVERSE_MAX_AGE_HOURS": "30",
              "MAX_TOTAL_SYMBOLS": "3", "MAX_SYMBOLS_PER_CONNECTION": "3",
              "KAFKA_ENABLE_AUTO_COMMIT": "false",
              "SP500_URL": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
              "LOG_TO_FILE": "false"}
    definitions = [
        ("SP500 Universe Refresh", "sp500-universe-weekdays", "ingestion", "/app",
         "polyhorizon/sp500_data/flow.py:sp500_universe_flow", "15 9 * * 1-5", {}),
        ("Finnhub-Ingestion-Pipeline", "ingestion-market-hours", "ingestion", "/app",
         "polyhorizon/ingestion/flow/producer_consumer_streaming_flow.py:finnhub_ingestion_flow", "29 9 * * 1-5", {}),
        ("spark-streaming-job-flow", "streaming-market-hours", "streaming", "/opt/app",
         "polyhorizon/streaming/tasks/spark_streaming_flow.py:spark_streaming_flow", "25 9 * * 1-5", {"qualification_only": True}),
        ("feature-store-flow", "feature-store-after-close", "features", "/app",
         "polyhorizon/features/flow/feature_store_flow.py:feature_store_flow", None,
         {"apply_changes": True, "materialize": True}),
        ("publication-deadline-watchdog", "publication-deadline-hourly", "ingestion", "/app",
         "polyhorizon/ingestion/flow/publication_watchdog.py:publication_deadline_watchdog", "5 * * * *", {}),
    ]
    images = {"ingestion": "polyhorizon-ingestion:reliability-fix", "streaming": "spark-custom:session-qualified-local",
              "features": "polyhorizon-features:verified-local"}
    if low_memory:
        common.update(KAFKA_SERVERS="kafka-broker-1:29092,kafka-broker-2:29093",
                      KAFKA_BOOTSTRAP_SERVERS="kafka-broker-1:29092,kafka-broker-2:29093",
                      POLYHORIZON_LOW_MEMORY="1")
        images["streaming"] = "spark-custom:low-memory-local"
        images["features"] = "polyhorizon-features:low-memory-local"
    result = []
    for flow, name, kind, path, entry, cron, parameters in definitions:
        volumes = [f"{env_file}:/app/.env:ro"]
        if path != "/app":
            volumes.append(f"{env_file}:{path}/.env:ro")
        result.append(dict(flow=flow, name=name, pool=f"local-{kind}-pool", path=path,
                           entrypoint=entry, cron=cron, parameters=parameters,
                           job_variables={"image": images[kind], "image_pull_policy": "Never",
                               "networks": [network], "volumes": volumes, "auto_remove": False,
                               "env": dict(common, PYTHONPATH=path)}))
    if low_memory:
        for item in result:
            # Driver/container overhead is separate from the Java heap.
            item["job_variables"]["mem_limit"] = "1536m" if item["pool"] == "local-streaming-pool" else "512m"
    return result


COLLECTION_DEPLOYMENTS = {"sp500-universe-weekdays", "ingestion-market-hours", "streaming-market-hours"}


def register(env_file, network, activate=False, low_memory=False):
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import WorkPoolCreate, DeploymentScheduleCreate
    from prefect.client.schemas.schedules import CronSchedule
    from prefect_docker.worker import DockerWorker
    definitions = specifications(env_file, network, low_memory)
    with get_client(sync_client=True) as client:
        for pool in sorted({d["pool"] for d in definitions}):
            # The long-running ingestion flow must not block deadline checks.
            limit = 2 if pool == "local-ingestion-pool" else 1
            client.create_work_pool(WorkPoolCreate(name=pool, type="docker", concurrency_limit=limit,
                base_job_template=DockerWorker.get_default_base_job_template()), overwrite=True)
        for d in definitions:
            enabled = activate and d["name"] in COLLECTION_DEPLOYMENTS
            flow_id = client.create_flow_from_name(d["flow"])
            schedules = [DeploymentScheduleCreate(schedule=CronSchedule(cron=d["cron"],
                         timezone="America/New_York"), active=enabled)] if d["cron"] else []
            deployment_id = client.create_deployment(flow_id=flow_id, name=d["name"],
                work_pool_name=d["pool"], path=d["path"], entrypoint=d["entrypoint"],
                parameters=d["parameters"], job_variables=d["job_variables"],
                schedules=schedules, paused=not enabled, concurrency_limit=1,
                tags=["local", "qualification-pending"],
                pull_steps=[{"prefect.deployments.steps.set_working_directory": {"directory": d["path"]}}])
            saved = client.read_deployment(deployment_id)
            assert saved.job_variables["image"] == d["job_variables"]["image"]
            print(d["name"], deployment_id, "schedules_active=" + str(enabled))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", required=True, help="Absolute host path to job .env")
    parser.add_argument("--network", default="polyhorizon_ml-platform")
    parser.add_argument("--activate", action="store_true", help="Enable universe and collection schedules only; never feature publication or training")
    parser.add_argument("--low-memory", action="store_true")
    args = parser.parse_args()
    if not Path(args.env_file).is_absolute():
        parser.error("--env-file must be an absolute host path")
    register(args.env_file, args.network, args.activate, args.low_memory)
