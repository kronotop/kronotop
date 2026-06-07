/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.namespace;

import com.kronotop.BaseHandlerTest;
import com.kronotop.namespace.handlers.NamespaceMetadata;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NamespaceUtilConcurrencyTest extends BaseHandlerTest {

    private static final int THREAD_COUNT = 16;

    @Test
    void shouldHandleConcurrentCreateOfSameNamespace() throws InterruptedException {
        // Behavior: When multiple threads concurrently create the same namespace,
        // exactly one succeeds and the rest get NamespaceAlreadyExistsException.
        String namespace = "concurrent.same.target";

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger alreadyExistsCount = new AtomicInteger(0);
        AtomicReference<Exception> unexpectedException = new AtomicReference<>();

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    NamespaceUtil.create(context, namespace);
                    successCount.incrementAndGet();
                } catch (NamespaceAlreadyExistsException e) {
                    alreadyExistsCount.incrementAndGet();
                } catch (Exception e) {
                    unexpectedException.compareAndSet(null, e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "All threads should complete within 30 seconds");
        executor.shutdown();

        assertNull(unexpectedException.get(), "No unexpected exceptions should occur");
        assertThat(successCount.get()).isEqualTo(1);
        assertThat(alreadyExistsCount.get()).isEqualTo(THREAD_COUNT - 1);

        assertThat(NamespaceUtil.exists(context, List.of("concurrent", "same", "target"))).isTrue();
        NamespaceMetadata metadata = NamespaceUtil.readMetadata(context, namespace);
        assertThat(metadata.id()).isNotNull();
        assertThat(metadata.name()).isEqualTo(namespace);
    }

    @Test
    void shouldHandleConcurrentCreateOfDifferentNamespaces() throws InterruptedException {
        // Behavior: When multiple threads concurrently create different namespaces, all succeed.
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicReference<Exception> unexpectedException = new AtomicReference<>();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    NamespaceUtil.create(context, "concurrent.diff.ns" + threadId);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    unexpectedException.compareAndSet(null, e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "All threads should complete within 30 seconds");
        executor.shutdown();

        assertNull(unexpectedException.get(), "No unexpected exceptions should occur");
        assertThat(successCount.get()).isEqualTo(THREAD_COUNT);

        Set<UUID> ids = new HashSet<>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            String namespace = "concurrent.diff.ns" + i;
            assertThat(NamespaceUtil.exists(context, List.of("concurrent", "diff", "ns" + i))).isTrue();
            NamespaceMetadata metadata = NamespaceUtil.readMetadata(context, namespace);
            assertThat(metadata.id()).isNotNull();
            assertThat(metadata.name()).isEqualTo(namespace);
            ids.add(metadata.id());
        }
        assertThat(ids).hasSize(THREAD_COUNT);
    }

    @Test
    void shouldHandleConcurrentCreateWithSharedParent() throws InterruptedException {
        // Behavior: When multiple threads concurrently create child namespaces under the same
        // non-existent parent, all eventually succeed via retry logic. The shared parent gets
        // a single consistent UUID.
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicReference<Exception> unexpectedException = new AtomicReference<>();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    NamespaceUtil.create(context, "shared.parent.child" + threadId);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    unexpectedException.compareAndSet(null, e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "All threads should complete within 30 seconds");
        executor.shutdown();

        assertNull(unexpectedException.get(), "No unexpected exceptions should occur");
        assertThat(successCount.get()).isEqualTo(THREAD_COUNT);

        NamespaceMetadata parentMetadata = NamespaceUtil.readMetadata(context, "shared.parent");
        assertThat(parentMetadata.id()).isNotNull();

        Set<UUID> childIds = new HashSet<>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            String childNamespace = "shared.parent.child" + i;
            assertThat(NamespaceUtil.exists(context, List.of("shared", "parent", "child" + i))).isTrue();
            NamespaceMetadata childMetadata = NamespaceUtil.readMetadata(context, childNamespace);
            assertThat(childMetadata.id()).isNotNull();
            assertThat(childMetadata.name()).isEqualTo(childNamespace);
            childIds.add(childMetadata.id());
        }
        assertThat(childIds).hasSize(THREAD_COUNT);
    }
}
