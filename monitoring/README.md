# PolyHorizon Monitoring (Root)

This folder is the **source of truth** for observability in PolyHorizon. It contains
Prometheus metrics and SLO rules, Alertmanager routing, Loki storage, Grafana Alloy
Docker-log collection, and provisioned Grafana datasources and dashboards.

## Structure
- `prometheus.yml` — scrape configuration + alert rule includes
- `alerts/` — Prometheus alert rules (ingestion, serving, streaming, features, training)
- `grafana/` — datasource + dashboards provisioning
- `alertmanager.yml` — alert routing and notification settings
- `loki.yml` — single-node log storage and seven-day retention
- `alloy.config` — Docker discovery, structured-log parsing, and Loki forwarding
- `secrets/` — ignored runtime notification secrets; only examples are committed

## Alert Rules
Current alert rules are loaded from:
- `alerts/ingestion-alerts.yml`
- `alerts/serving_rules.yaml`
- `alerts/streaming_rules.yaml`
- `alerts/features_rules.yaml`
- `alerts/training_rules.yaml`
- `alerts/postgres_rules.yaml`
- `alerts/platform_rules.yaml`
- `alerts/slo_rules.yaml`

## Grafana Dashboards
Dashboards are provisioned from:
- `grafana/dashboards/polyhorizon/` — 30-second Mission Control landing page
- `grafana/dashboards/spark/`
- `grafana/dashboards/kafka/`
- `grafana/dashboards/ingestion/`
- `grafana/dashboards/serving/`
- `grafana/dashboards/features/`
- `grafana/dashboards/training/`

Mission Control (`/d/polyhorizon-mission-control`) is the live telemetry landing
page. It links to the current serving, features, training, and ingestion
dashboards. The features dashboard queries were aligned with emitted metric
names; the legacy imported Kafka/Spark JSON dashboards have not been migrated
and are not linked as verified drill-downs. The top-level page uses explicit
no-data behavior: absent metrics are unknown, not healthy. Its champion-match
metric reflects the scraped serving process, not all replicas.

Feature publication emits success, last event time, and interior gaps between
observed 30-minute bars for the latest session. Interior gaps do not detect a
missing opening bar; use the operator `/ops` calendar comparison for exact
latest-session completeness. Feature, training, and streaming Pushgateway
publishers add individual gauge families instead of replacing the entire job.
Realized forecast quality and fleet-wide activation still need durable feeds.

## Restart After Changes
```bash
docker compose restart prometheus alertmanager loki alloy grafana
```

For a local dashboard-only startup, run `docker compose up -d prometheus
alertmanager grafana` and open
`http://localhost:3000/d/polyhorizon-mission-control`. The JSON dashboard is
mounted read-only and Grafana discovers it without an application build, but
new publication and serving metrics appear only after the corresponding
features/serving images are rebuilt and a real publication/readiness check runs.

Prometheus scrapes service metrics only on the internal Compose network. The
public Traefik router does not expose `/metrics`.

For production notifications, copy the example Slack webhook file to a
non-versioned path, replace the placeholder, and set
`ALERTMANAGER_SLACK_WEBHOOK_FILE` in `.env.prod`.
