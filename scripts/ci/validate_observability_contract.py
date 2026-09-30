"""Validate observability and edge-routing production invariants."""

from pathlib import Path
import json

import yaml


ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.prod.yaml").read_text())
    services = compose["services"]

    required_services = {
        "prometheus",
        "alertmanager",
        "prometheus-pushgateway",
        "grafana",
        "loki",
        "alloy",
        "docker-socket-proxy",
        "traefik",
    }
    if missing := required_services - set(services):
        raise SystemExit(f"Observability services missing: {sorted(missing)}")

    prometheus = yaml.safe_load((ROOT / "monitoring/prometheus.yml").read_text())
    alert_targets = (
        prometheus.get("alerting", {})
        .get("alertmanagers", [{}])[0]
        .get("static_configs", [{}])[0]
        .get("targets", [])
    )
    if "alertmanager:9093" not in alert_targets:
        raise SystemExit("Prometheus is not connected to Alertmanager")

    scrape_jobs = {job["job_name"] for job in prometheus.get("scrape_configs", [])}
    expected_jobs = {
        "model-serving",
        "traefik",
        "postgres",
        "loki",
        "alloy",
        "pushgateway",
    }
    if missing_jobs := expected_jobs - scrape_jobs:
        raise SystemExit(f"Prometheus scrape jobs missing: {sorted(missing_jobs)}")

    for rule_ref in prometheus.get("rule_files", []):
        rule_path = ROOT / rule_ref.replace("/etc/prometheus/", "monitoring/")
        if not rule_path.exists():
            raise SystemExit(f"Prometheus rule file does not exist: {rule_path}")
        rules = yaml.safe_load(rule_path.read_text())
        if not rules or "groups" not in rules:
            raise SystemExit(f"Invalid Prometheus rule structure: {rule_path}")

    serving_labels = services["model-serving"].get("labels", [])
    serving_rule = next(label for label in serving_labels if ".routers.serving.rule=" in label)
    if "/metrics" in serving_rule:
        raise SystemExit("Serving metrics must not be exposed through the public router")

    traefik = services["traefik"]
    commands = set(traefik.get("command", []))
    required_commands = {
        "--providers.docker.endpoint=tcp://docker-socket-proxy:2375",
        "--entrypoints.web.http.redirections.entrypoint.to=websecure",
        "--accesslog.format=json",
        "--metrics.prometheus=true",
    }
    if missing_commands := required_commands - commands:
        raise SystemExit(f"Traefik hardening flags missing: {sorted(missing_commands)}")
    if any("docker.sock" in volume for volume in traefik.get("volumes", [])):
        raise SystemExit("Traefik must not mount the Docker socket directly")

    labels = "\n".join(traefik.get("labels", []))
    for capability in (
        "edge-security.headers.stsSeconds",
        "api-ratelimit.ratelimit.average",
        "api-inflight.inflightreq.amount",
        "edge-compress.compress",
    ):
        if capability not in labels:
            raise SystemExit(f"Traefik middleware missing: {capability}")

    alertmanager = yaml.safe_load((ROOT / "monitoring/alertmanager.yml").read_text())
    if alertmanager.get("global", {}).get("slack_api_url_file") != (
        "/run/secrets/alertmanager_slack_url"
    ):
        raise SystemExit("Alertmanager must read its webhook from a mounted secret file")

    alloy = (ROOT / "monitoring/alloy.config").read_text()
    if "loki.source.docker" not in alloy or "docker-socket-proxy:2375" not in alloy:
        raise SystemExit("Alloy Docker log collection is incomplete")

    grafana_volumes = "\n".join(services["grafana"].get("volumes", []))
    for mount in (
        "/etc/grafana/provisioning/datasources",
        "/etc/grafana/provisioning/dashboards",
        "/var/lib/grafana/dashboards",
    ):
        if mount not in grafana_volumes:
            raise SystemExit(f"Grafana provisioning mount missing: {mount}")

    provisioned = (ROOT / "monitoring/grafana/dashboards/dashboards.yaml").read_text()
    if "/var/lib/grafana/dashboards/polyhorizon" not in provisioned:
        raise SystemExit("Mission Control dashboard is not provisioned")
    dashboard = json.loads((ROOT / "monitoring/grafana/dashboards/polyhorizon/mission-control.json").read_text())
    if dashboard.get("uid") != "polyhorizon-mission-control" or dashboard.get("refresh") != "30s":
        raise SystemExit("Mission Control dashboard identity or refresh is invalid")
    expressions = "\n".join(target["expr"] for panel in dashboard["panels"]
                            for target in panel.get("targets", []))
    for marker in ("serving_champion_match", "features_publication_success",
                   "features_interior_session_gaps", "training_last_run_success",
                   "serving_forecast_stale_rejections_total"):
        if marker not in expressions:
            raise SystemExit(f"Mission Control is missing {marker}")


if __name__ == "__main__":
    main()
