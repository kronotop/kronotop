# Observability

Grafana dashboards for Kronotop. Import the JSON files in `grafana/` into your Grafana.

![Kronotop Grafana dashboard](grafana-dashboard.png)

- `kronotop-overview.json`: cluster members, clients, traffic, transactions.
- `kronotop-jvm.json`: heap, non-heap, direct buffers, GC, allocation rate.
- `kronotop-data-models.json`: Bucket, ZMap, vector index and index maintenance activity.

## Requirements

- OTLP export enabled in Kronotop (`observability.otlp.enabled`, `observability.otlp.endpoint`).
- An OpenTelemetry Collector that receives OTLP/HTTP and exposes the Prometheus exporter with `resource_to_telemetry_conversion: enabled`.
- Prometheus scraping the collector, added to Grafana as a datasource with `uid: prometheus`.

## Metric naming

Kronotop sends the numeric `INFO` fields. The collector produces Prometheus names:

- Prefix `kronotop_`.
- Counters end with `_total`, byte values end with `_bytes`.
- Labels: `kronotop_cluster_name`, `instance` (member id), `service_version`, `kronotop_run_id`.
- `kronotop_run_id` identifies one process lifetime. It changes on every restart of the member, so a new value marks a restart and the start of a new counter series.

For a local stack with collector, Prometheus and Grafana, see `docker/observability/`.
