"""
Prefect flow for managing Spark streaming jobs in production.
"""
import os
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

from prefect import flow, task
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from polyhorizon.streaming.src.market_schedule import get_market_session, is_market_open


@task(name="submit-spark-job", retries=3, retry_delay_seconds=60)
def submit_spark_job(job_path: str, job_name: str, extra_conf: dict = None):
    """
    Submit a Spark job to the cluster via spark-submit.
    
    Args:
        job_path: Path to the Python job file (e.g., '/opt/spark/jobs/streaming_job.py')
        job_name: Name of the Spark application
        extra_conf: Additional Spark configurations
    """
    from dotenv import load_dotenv
    load_dotenv("/app/.env")
    spark_conf = [
        "--master", "spark://spark-master:7077",
        # Spark standalone does not support cluster deploy mode for Python apps.
        "--deploy-mode", "client",
        "--name", job_name,
        "--conf", "spark.sql.streaming.checkpointLocation=s3a://polyhorizon-streamingdata/checkpoints/",
    ]
    
    # Add extra configurations
    if extra_conf:
        for key, value in extra_conf.items():
            spark_conf.extend(["--conf", f"{key}={value}"])
    
    # Add the job file
    spark_conf.append(job_path)
    
    # The Prefect job itself uses the release-pinned Spark image. Running
    # spark-submit directly avoids a privileged nested-Docker control path.
    with tempfile.TemporaryDirectory(prefix="dataset-release-") as directory:
        output = Path(directory) / "release.json"
        subprocess.run(
            ["/opt/spark/bin/spark-submit", *spark_conf], check=True,
            env={**os.environ, "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
                 "DATASET_RELEASE_FILE": str(output)},
        )
        from polyhorizon.core.dataset_release import DatasetRelease
        return DatasetRelease.model_validate_json(output.read_text()).model_dump(mode="json")


@task(name="publish-completed-dataset", retries=2, retry_delay_seconds=60)
def publish_completed_dataset(release: dict):
    from prefect.deployments import run_deployment
    from polyhorizon.core.dataset_release import DatasetRelease
    manifest = DatasetRelease.model_validate(release)
    run = run_deployment(
        name="feature-store-flow/feature-store-after-close",
        parameters={"dataset_release": manifest.model_dump(mode="json")},
        timeout=7200,
    )
    if run.state is None or not run.state.is_completed():
        raise RuntimeError(f"Feature publication did not complete: {run.id}")
    return {"dataset_id": str(manifest.dataset_id), "feature_flow_run_id": str(run.id)}


@task(name="market-hours-check")
def market_hours_check(require_open: bool = False, timezone: str = "America/New_York") -> bool:
    now = datetime.now(ZoneInfo(timezone))
    session = get_market_session(now, "NYSE", timezone)
    if session is None:
        return False

    if not require_open:
        return True

    return is_market_open(now, "NYSE", timezone)


@task(name="is-streaming-active")
def is_streaming_active(app_name: str) -> bool:
    import requests

    spark_master_url = "http://spark-master:8080"
    response = requests.get(f"{spark_master_url}/json/", timeout=5)
    response.raise_for_status()
    apps = response.json().get("activeapps", [])
    return any(app.get("name") == app_name for app in apps)


@task(name="streaming-health-check")
def streaming_health_check(status: str, detail: str = "") -> None:
    registry = CollectorRegistry()
    gauge = Gauge(
        "polyhorizon_streaming_flow_status",
        "Status of Prefect streaming flow",
        ["status", "detail"],
        registry=registry,
    )
    gauge.labels(status=status, detail=detail).set(1)
    push_to_gateway("prometheus-pushgateway:9091", job="streaming-flow", registry=registry)


@task(name="monitor-spark-job")
def monitor_spark_job(app_id: str):
    """
    Monitor Spark job status via REST API.
    
    Args:
        app_id: Spark application ID
    """
    import requests
    
    spark_master_url = "http://spark-master:8080"
    response = requests.get(f"{spark_master_url}/json/")
    apps = response.json().get("activeapps", [])
    
    for app in apps:
        if app["id"] == app_id:
            return {
                "status": "running",
                "duration": app.get("duration"),
                "cores": app.get("coresgranted"),
            }
    
    return {"status": "not_found"}


@flow(
    name="spark-streaming-job-flow",
    retries=0,
    retry_delay_seconds=60,
    timeout_seconds=10 * 60 * 60,
)
def spark_streaming_flow(job_name: str = "polyhorizon-streaming-job"):
    """
    Main Prefect flow for managing Spark streaming jobs.
    
    This flow:
    1. Submits the Spark job to the cluster
    2. Monitors the job status
    3. Handles failures and retries
    """
    job_path = "/opt/spark/jobs/streaming_job.py"
    
    if not market_hours_check(require_open=False):
        streaming_health_check(status="skipped", detail="market_closed")
        return {"status": "skipped", "reason": "market_closed"}

    if is_streaming_active(job_name):
        streaming_health_check(status="already_running", detail=job_name)
        return {"status": "already_running"}

    streaming_health_check(status="starting", detail=job_name)
    result = submit_spark_job(job_path=job_path, job_name=job_name)
    streaming_health_check(status="submitted", detail=str(datetime.utcnow().isoformat()))

    return publish_completed_dataset(result)


@flow(
    name="spark-streaming-health-flow",
    retries=0,
    retry_delay_seconds=60,
    # A recovery run owns the Python Spark driver in standalone client mode and
    # therefore remains alive for the rest of the market session.
    timeout_seconds=10 * 60 * 60,
)
def spark_streaming_health_flow(job_name: str = "polyhorizon-streaming-job"):
    if not market_hours_check(require_open=True):
        streaming_health_check(status="skipped", detail="market_closed")
        return {"status": "skipped", "reason": "market_closed"}

    if is_streaming_active(job_name):
        streaming_health_check(status="healthy", detail=job_name)
        return {"status": "healthy"}

    streaming_health_check(status="restarting", detail=job_name)
    result = submit_spark_job(job_path="/opt/spark/jobs/streaming_job.py", job_name=job_name)
    streaming_health_check(status="submitted", detail=str(datetime.utcnow().isoformat()))
    return {"status": "restarted", "result": publish_completed_dataset(result)}


if __name__ == "__main__":
    # For testing locally
    spark_streaming_flow()
