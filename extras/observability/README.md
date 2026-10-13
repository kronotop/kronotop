# Observability

Grafana dashboards for Kronotop. Import the JSON files in [grafana/](grafana/) into your Grafana.

![Kronotop Grafana dashboard](grafana-dashboard.png)

- `kronotop-overview.json`: cluster members, clients, traffic, transactions.
- `kronotop-jvm.json`: heap, non-heap, direct buffers, GC, allocation rate.
- `kronotop-data-models.json`: Bucket, ZMap, vector index and index maintenance activity.

## Requirements

- OTLP export enabled in Kronotop (`observability.otlp.enabled`, `observability.otlp.endpoint`).
- An OpenTelemetry Collector that receives OTLP/HTTP and exposes the Prometheus exporter with `resource_to_telemetry_conversion: enabled`.
- Prometheus scraping the collector, added to Grafana as a datasource with `uid: prometheus`.

For a local stack with collector, Prometheus and Grafana, see [docker/observability](../../docker/observability).

See [docs/monitoring/opentelemetry.md](../../docs/monitoring/opentelemetry.md) for metric names, labels and query examples.
