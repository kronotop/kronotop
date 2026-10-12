/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.metrics;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;
import com.kronotop.Context;
import com.kronotop.core.InfoCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

/**
 * Pushes the numeric INFO fields to an OTLP/HTTP endpoint as JSON.
 * Only the keys listed in SUMS and GAUGES are exported, the rest of the
 * INFO report is ignored. Every failure is logged and swallowed so the
 * scheduler keeps the task alive.
 */
public class OtlpExportTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(OtlpExportTask.class);
    private static final String METRIC_PREFIX = "kronotop.";
    private static final int CUMULATIVE = 2;

    // Monotonic counters since process start.
    private static final Set<String> SUMS = Set.of(
            "process_cpu_time_msec",
            "gc_count",
            "gc_time_msec",
            "gc_freed_memory",
            "total_allocated_memory",
            "external_total_commands_processed",
            "external_commands_failed",
            "external_read_bytes",
            "external_written_bytes",
            "internal_total_commands_processed",
            "internal_commands_failed",
            "internal_read_bytes",
            "internal_written_bytes",
            "transactions_created",
            "transactions_committed",
            "transactions_conflicted",
            "transactions_commit_failed",
            "transactions_rolled_back",
            "transaction_reads",
            "transaction_range_reads",
            "transaction_writes",
            "transaction_deletes",
            "transaction_range_deletes",
            "transaction_mutations",
            "transaction_bytes_read",
            "transaction_bytes_written",
            "bucket_inserts",
            "bucket_queries",
            "bucket_updates",
            "bucket_deletes",
            "bucket_advances",
            "bucket_vector_searches",
            "bucket_purges",
            "bucket_removes",
            "plan_cache_hits",
            "plan_cache_misses",
            "index_maintenance_processed_entries",
            "index_maintenance_retried_conflicts",
            "vector_retried_ops",
            "zmap_sets",
            "zmap_gets",
            "zmap_range_reads",
            "zmap_deletes",
            "zmap_mutations",
            "zmap_watch_fires"
    );

    // Point-in-time values.
    private static final Set<String> GAUGES = Set.of(
            "uptime_in_seconds",
            "connected_clients",
            "clients_in_transaction",
            "clients_in_multi",
            "snapshot_read_clients",
            "resp2_clients",
            "resp3_clients",
            "heap_used_memory",
            "heap_committed_memory",
            "heap_max_memory",
            "non_heap_used_memory",
            "non_heap_committed_memory",
            "non_heap_max_memory",
            "direct_buffer_count",
            "direct_buffer_used_memory",
            "direct_buffer_total_capacity",
            "mapped_buffer_count",
            "mapped_buffer_total_capacity",
            "netty_used_direct_memory",
            "netty_used_heap_memory",
            "transactions_active",
            "transactions_watching",
            "oldest_active_transaction_usec",
            "max_commit_latency_usec",
            "plan_cache_size",
            "open_cursors",
            "index_maintenance_workers",
            "vector_indexes",
            "vector_bytes_used",
            "vector_pending_retries",
            "zmap_watches",
            "zmap_watched_keys",
            "zmap_watch_waiters",
            "known_members",
            "alive_members",
            "bucket_shards",
            "stash_shards",
            "primary_shards",
            "standby_shards",
            "task_count",
            "running_tasks",
            "volume_count"
    );

    // Byte-sized values. Exported with unit "By" so the Prometheus name ends with _bytes.
    private static final Set<String> BYTES = Set.of(
            "gc_freed_memory",
            "total_allocated_memory",
            "external_read_bytes",
            "external_written_bytes",
            "internal_read_bytes",
            "internal_written_bytes",
            "transaction_bytes_read",
            "transaction_bytes_written",
            "heap_used_memory",
            "heap_committed_memory",
            "heap_max_memory",
            "non_heap_used_memory",
            "non_heap_committed_memory",
            "non_heap_max_memory",
            "direct_buffer_used_memory",
            "direct_buffer_total_capacity",
            "mapped_buffer_total_capacity",
            "netty_used_direct_memory",
            "netty_used_heap_memory",
            "vector_bytes_used"
    );

    private final InfoReport report;
    private final URI endpoint;
    private final HttpClient client;
    private final ObjectMapper mapper = new ObjectMapper();
    private final long startTimeUnixNano;

    public OtlpExportTask(Context context) {
        this(context, context.getConfig().getString("observability.otlp.endpoint"));
    }

    OtlpExportTask(Context context, String endpoint) {
        this.report = new InfoReport(context);
        this.endpoint = URI.create(endpoint.replaceAll("/+$", "") + "/v1/metrics");
        this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        this.startTimeUnixNano = ManagementFactory.getRuntimeMXBean().getStartTime() * 1_000_000L;
    }

    @Override
    public void run() {
        try {
            byte[] body = mapper.writeValueAsBytes(build(report.collect()));
            HttpRequest request = HttpRequest.newBuilder(endpoint)
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build();
            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() / 100 != 2) {
                LOGGER.warn("OTLP export to {} failed with status {}", endpoint, response.statusCode());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOGGER.warn("OTLP export to {} failed: {}", endpoint, e.toString());
        }
    }

    ObjectNode build(InfoCollector collector) {
        Map<String, Map<String, String>> sections = collector.sections();
        long now = System.currentTimeMillis() * 1_000_000L;

        ObjectNode root = mapper.createObjectNode();
        ObjectNode resourceMetrics = root.putArray("resourceMetrics").addObject();
        ArrayNode attributes = resourceMetrics.putObject("resource").putArray("attributes");
        Map<String, String> server = sections.getOrDefault(InfoReport.SERVER_SECTION, Map.of());
        Map<String, String> kronotop = sections.getOrDefault(InfoReport.KRONOTOP_SECTION, Map.of());
        attribute(attributes, "service.name", "kronotop");
        attribute(attributes, "service.version", server.get("kronotop_version"));
        attribute(attributes, "service.instance.id", kronotop.get("member_id"));
        attribute(attributes, "kronotop.cluster_name", kronotop.get("cluster_name"));
        attribute(attributes, "kronotop.run_id", server.get("run_id"));

        ObjectNode scopeMetrics = resourceMetrics.putArray("scopeMetrics").addObject();
        scopeMetrics.putObject("scope").put("name", "kronotop");
        ArrayNode metrics = scopeMetrics.putArray("metrics");

        for (Map<String, String> section : sections.values()) {
            for (Map.Entry<String, String> field : section.entrySet()) {
                String key = field.getKey();
                boolean sum = SUMS.contains(key);
                if (!sum && !GAUGES.contains(key)) {
                    continue;
                }
                long value;
                try {
                    value = Long.parseLong(field.getValue());
                } catch (NumberFormatException e) {
                    continue;
                }
                if (value < 0) {
                    // -1 means the JVM does not report this value.
                    continue;
                }
                ObjectNode metric = metrics.addObject();
                metric.put("name", METRIC_PREFIX + key);
                if (BYTES.contains(key)) {
                    metric.put("unit", "By");
                }
                ObjectNode point;
                if (sum) {
                    ObjectNode node = metric.putObject("sum");
                    node.put("aggregationTemporality", CUMULATIVE);
                    node.put("isMonotonic", true);
                    point = node.putArray("dataPoints").addObject();
                    point.put("startTimeUnixNano", Long.toString(startTimeUnixNano));
                } else {
                    point = metric.putObject("gauge").putArray("dataPoints").addObject();
                }
                point.put("timeUnixNano", Long.toString(now));
                point.put("asInt", Long.toString(value));
            }
        }
        return root;
    }

    private static void attribute(ArrayNode attributes, String key, String value) {
        if (value == null) {
            return;
        }
        ObjectNode attribute = attributes.addObject();
        attribute.put("key", key);
        attribute.putObject("value").put("stringValue", value);
    }
}
