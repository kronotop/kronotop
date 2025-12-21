/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.worker;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class WorkerRegistryTest {

    private static class TestWorker implements Worker {
        private final String tag;

        TestWorker(String tag) {
            this.tag = tag;
        }

        @Override
        public String getTag() {
            return tag;
        }

        @Override
        public void shutdown() {
        }

        @Override
        public boolean await(long timeout, TimeUnit unit) {
            return true;
        }
    }

    @Test
    void shouldPutWorker() {
        WorkerRegistry registry = new WorkerRegistry();
        Worker worker = new TestWorker("test-worker");

        registry.put("test.namespace", worker);

        List<Worker> retrieved = registry.get("test.namespace", "test-worker");
        assertEquals(1, retrieved.size());
        assertSame(worker, retrieved.getFirst());
    }

    @Test
    void shouldPutMultipleWorkersWithSameTag() {
        WorkerRegistry registry = new WorkerRegistry();
        Worker worker1 = new TestWorker("test-worker");
        Worker worker2 = new TestWorker("test-worker");

        registry.put("test.namespace", worker1);
        registry.put("test.namespace", worker2);

        List<Worker> retrieved = registry.get("test.namespace", "test-worker");
        assertEquals(2, retrieved.size());
        assertTrue(retrieved.contains(worker1));
        assertTrue(retrieved.contains(worker2));
    }

    @Test
    void shouldGetWorkers() {
        WorkerRegistry registry = new WorkerRegistry();
        Worker worker1 = new TestWorker("worker-1");
        Worker worker2 = new TestWorker("worker-2");

        registry.put("test.namespace", worker1);
        registry.put("test.namespace", worker2);

        assertEquals(1, registry.get("test.namespace", "worker-1").size());
        assertEquals(1, registry.get("test.namespace", "worker-2").size());
    }

    @Test
    void shouldReturnEmptyListWhenNamespaceNotFound() {
        WorkerRegistry registry = new WorkerRegistry();

        List<Worker> result = registry.get("non.existent.namespace", "test-worker");

        assertTrue(result.isEmpty());
    }

    @Test
    void shouldReturnEmptyListWhenWorkerNotFound() {
        WorkerRegistry registry = new WorkerRegistry();
        Worker worker = new TestWorker("existing-worker");
        registry.put("test.namespace", worker);

        List<Worker> result = registry.get("test.namespace", "non-existent-worker");

        assertTrue(result.isEmpty());
    }

    @Test
    void shouldGetWorkersByPrefix() {
        WorkerRegistry registry = new WorkerRegistry();
        Worker worker1 = new TestWorker("worker-1");
        Worker worker2 = new TestWorker("worker-2");
        Worker worker3 = new TestWorker("worker-3");

        registry.put("app.service.a", worker1);
        registry.put("app.service.b", worker2);
        registry.put("other.namespace", worker3);

        var result = registry.get("app.service");

        assertEquals(2, result.size());
        assertTrue(result.containsKey("app.service.a"));
        assertTrue(result.containsKey("app.service.b"));
    }

    @Test
    void shouldReturnEmptyMapWhenNoWorkersMatchPrefix() {
        WorkerRegistry registry = new WorkerRegistry();
        Worker worker = new TestWorker("worker");
        registry.put("app.service", worker);

        var result = registry.get("non.matching.prefix");

        assertTrue(result.isEmpty());
    }

    @Test
    void shouldRemoveWorker() {
        WorkerRegistry registry = new WorkerRegistry();
        Worker worker1 = new TestWorker("worker");
        Worker worker2 = new TestWorker("worker");
        registry.put("test.namespace", worker1);
        registry.put("test.namespace", worker2);

        boolean removed = registry.remove("test.namespace", worker1);

        assertTrue(removed);
        List<Worker> remaining = registry.get("test.namespace", "worker");
        assertEquals(1, remaining.size());
        assertSame(worker2, remaining.getFirst());
    }

    @Test
    void shouldReturnFalseWhenRemovingFromNonExistentNamespace() {
        WorkerRegistry registry = new WorkerRegistry();
        Worker worker = new TestWorker("worker");

        boolean removed = registry.remove("non.existent.namespace", worker);

        assertFalse(removed);
    }

    @Test
    void shouldReturnFalseWhenRemovingNonExistentWorker() {
        WorkerRegistry registry = new WorkerRegistry();
        Worker worker1 = new TestWorker("existing-worker");
        Worker worker2 = new TestWorker("non-existent-worker");
        registry.put("test.namespace", worker1);

        boolean removed = registry.remove("test.namespace", worker2);

        assertFalse(removed);
    }

    @Test
    void shouldRemoveWorkersByPrefix() {
        WorkerRegistry registry = new WorkerRegistry();
        Worker worker1 = new TestWorker("worker-1");
        Worker worker2 = new TestWorker("worker-2");
        Worker worker3 = new TestWorker("worker-3");

        registry.put("app.service.a", worker1);
        registry.put("app.service.b", worker2);
        registry.put("other.namespace", worker3);

        registry.remove("app.service");

        assertTrue(registry.get("app.service").isEmpty());
        assertEquals(1, registry.get("other.namespace", "worker-3").size());
    }
}