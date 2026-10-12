# Local observability stack

OpenTelemetry Collector, Prometheus and Grafana for local development.

```
docker compose up -d
```

Ports are bound to `127.0.0.1`: Grafana on 3000, Prometheus on 9090, OTLP/HTTP on 4318. Kronotop must point `observability.otlp.endpoint` to `http://localhost:4318`.

Dashboards are loaded from `extras/observability/grafana`.
