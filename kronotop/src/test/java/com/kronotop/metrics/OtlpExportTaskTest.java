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

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import com.kronotop.BaseStandaloneInstanceTest;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class OtlpExportTaskTest extends BaseStandaloneInstanceTest {
    private final AtomicReference<byte[]> body = new AtomicReference<>();
    private final AtomicReference<String> contentType = new AtomicReference<>();
    private final AtomicInteger status = new AtomicInteger(200);
    private HttpServer server;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/metrics", exchange -> {
            body.set(exchange.getRequestBody().readAllBytes());
            contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            exchange.sendResponseHeaders(status.get(), -1);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private String endpoint() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private Map<String, JsonNode> metricsByName(JsonNode root) {
        Map<String, JsonNode> result = new HashMap<>();
        for (JsonNode metric : root.at("/resourceMetrics/0/scopeMetrics/0/metrics")) {
            result.put(metric.get("name").asString(), metric);
        }
        return result;
    }

    @Test
    void shouldPostGaugesAndSumsAsOtlpJson() {
        // Behavior: one run posts the allowlisted INFO fields as OTLP gauges and cumulative sums
        new OtlpExportTask(context, endpoint()).run();

        assertNotNull(body.get());
        assertEquals("application/json", contentType.get());
        JsonNode root = new ObjectMapper().readTree(body.get());

        JsonNode attributes = root.at("/resourceMetrics/0/resource/attributes");
        boolean serviceName = false;
        for (JsonNode attribute : attributes) {
            if (attribute.get("key").asString().equals("service.name")) {
                serviceName = attribute.at("/value/stringValue").asString().equals("kronotop");
            }
        }
        assertTrue(serviceName, "service.name=kronotop missing");

        Map<String, JsonNode> metrics = metricsByName(root);

        JsonNode gauge = metrics.get("kronotop.connected_clients");
        assertNotNull(gauge);
        assertTrue(gauge.has("gauge"));
        assertTrue(gauge.at("/gauge/dataPoints/0").has("asInt"));

        assertFalse(gauge.has("unit"));
        assertEquals("By", metrics.get("kronotop.heap_used_memory").get("unit").asString());

        JsonNode sum = metrics.get("kronotop.transactions_created");
        assertNotNull(sum);
        assertTrue(sum.at("/sum/isMonotonic").asBoolean());
        assertEquals(2, sum.at("/sum/aggregationTemporality").asInt());
        assertTrue(sum.at("/sum/dataPoints/0").has("startTimeUnixNano"));

        assertTrue(metrics.get("kronotop.thread_count").has("gauge"));
        assertTrue(metrics.get("kronotop.total_started_threads").at("/sum/isMonotonic").asBoolean());

        for (String name : metrics.keySet()) {
            assertFalse(name.endsWith("_human"), name);
            assertTrue(name.startsWith("kronotop."), name);
        }
        assertFalse(metrics.containsKey("kronotop.avg_commit_latency_usec"));
        assertFalse(metrics.containsKey("kronotop.server_time_usec"));
    }

    @Test
    void shouldNotThrowOnServerError() {
        // Behavior: a non-2xx response is logged and swallowed so the scheduler keeps the task
        status.set(500);
        assertDoesNotThrow(() -> new OtlpExportTask(context, endpoint()).run());
    }

    @Test
    void shouldNotThrowWhenEndpointIsDown() {
        // Behavior: a connection failure is logged and swallowed
        server.stop(0);
        assertDoesNotThrow(() -> new OtlpExportTask(context, endpoint()).run());
    }
}
