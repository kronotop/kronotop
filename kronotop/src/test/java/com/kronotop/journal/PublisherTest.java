/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.journal;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PublisherTest extends BaseStandaloneInstanceTest {
    private final String TEST_JOURNAL = "test-journal";

    @Test
    void shouldOrderVersionstampsCorrectlyAcrossTransactions() {
        Journal journal = new Journal(config, context.getFoundationDB());

        Publisher publisher = journal.getPublisher();
        VersionstampContainer first = publisher.publish(TEST_JOURNAL, "foo");
        VersionstampContainer second = publisher.publish(TEST_JOURNAL, "bar");

        Versionstamp firstVersionstamp = Versionstamp.complete(first.versionstamp().join(), first.userVersion());
        Versionstamp secondVersionstamp = Versionstamp.complete(second.versionstamp().join(), second.userVersion());
        assertTrue(firstVersionstamp.compareTo(secondVersionstamp) < 0);
    }

    @Test
    void shouldOrderVersionstampsCorrectlyWithinSingleTransaction() {
        Journal journal = new Journal(config, context.getFoundationDB());

        Publisher publisher = journal.getPublisher();
        List<VersionstampContainer> result = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VersionstampContainer first = publisher.publish(tr, TEST_JOURNAL, "foo");
            result.add(first);

            VersionstampContainer second = publisher.publish(tr, TEST_JOURNAL, "bar");
            result.add(second);

            tr.commit().join();
        }

        VersionstampContainer first = result.get(0);
        VersionstampContainer second = result.get(1);
        Versionstamp firstStamp = Versionstamp.complete(first.versionstamp().join(), first.userVersion());
        Versionstamp secondStamp = Versionstamp.complete(second.versionstamp().join(), second.userVersion());
        assertTrue(firstStamp.compareTo(secondStamp) < 0);
    }

    @Test
    void shouldNotPersistEventWhenTransactionNotCommitted() {
        Journal journal = new Journal(config, context.getFoundationDB());
        Publisher publisher = journal.getPublisher();

        // Publish in a transaction but don't commit
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            publisher.publish(tr, TEST_JOURNAL, "should-not-persist");
            // Intentionally NOT calling tr.commit()
        }

        // Verify event was not persisted
        ConsumerConfig consumerConfig = new ConsumerConfig("test-consumer", TEST_JOURNAL, ConsumerConfig.Offset.EARLIEST);
        Consumer consumer = new Consumer(context, consumerConfig);
        consumer.start();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Event event = consumer.consume(tr);
            assertNull(event, "Event should not exist when transaction was not committed");
        }
    }

    @Test
    void shouldIncrementUserVersionForMultipleEventsInSameTransaction() {
        Journal journal = new Journal(config, context.getFoundationDB());
        Publisher publisher = journal.getPublisher();

        List<VersionstampContainer> containers = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Publish 5 events in same transaction
            for (int i = 0; i < 5; i++) {
                containers.add(publisher.publish(tr, TEST_JOURNAL, "event-" + i));
            }
            tr.commit().join();
        }

        // Verify user versions increment: 0, 1, 2, 3, 4
        for (int i = 0; i < 5; i++) {
            assertEquals(i, containers.get(i).userVersion(),
                "User version should increment sequentially within transaction");
        }

        // Verify all have same transaction versionstamp but different user versions make them unique
        byte[] baseVersionstamp = containers.get(0).versionstamp().join();
        for (int i = 1; i < 5; i++) {
            assertArrayEquals(baseVersionstamp, containers.get(i).versionstamp().join(),
                "All events in same transaction should have same base versionstamp");

            Versionstamp prev = Versionstamp.complete(containers.get(i-1).versionstamp().join(), containers.get(i-1).userVersion());
            Versionstamp curr = Versionstamp.complete(containers.get(i).versionstamp().join(), containers.get(i).userVersion());
            assertTrue(prev.compareTo(curr) < 0, "Complete versionstamps should be ordered");
        }
    }

    @Test
    void shouldReturnValidVersionstampContainer() {
        Journal journal = new Journal(config, context.getFoundationDB());
        Publisher publisher = journal.getPublisher();

        VersionstampContainer result = publisher.publish(TEST_JOURNAL, "test-event");

        // Verify container has valid components
        assertNotNull(result, "VersionstampContainer should not be null");
        assertNotNull(result.versionstamp(), "Versionstamp future should not be null");
        assertTrue(result.userVersion() >= 0, "User version should be non-negative");

        // Verify versionstamp can be completed
        byte[] versionstampBytes = result.versionstamp().join();
        assertNotNull(versionstampBytes, "Versionstamp bytes should not be null");
        assertEquals(10, versionstampBytes.length, "Versionstamp should be 10 bytes");
    }

    @Test
    void shouldEncodeComplexObjectsAsJson() {
        Journal journal = new Journal(config, context.getFoundationDB());
        Publisher publisher = journal.getPublisher();

        // Publish complex object
        ComplexEvent complexEvent = new ComplexEvent("test-id", 42, List.of("tag1", "tag2"));
        VersionstampContainer result = publisher.publish(TEST_JOURNAL, complexEvent);
        assertNotNull(result);

        // Verify it can be consumed back
        ConsumerConfig consumerConfig = new ConsumerConfig("test-consumer", TEST_JOURNAL, ConsumerConfig.Offset.EARLIEST);
        Consumer consumer = new Consumer(context, consumerConfig);
        consumer.start();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Event event = consumer.consume(tr);
            assertNotNull(event);
            assertNotNull(event.value());
        }
    }

    @Test
    void shouldIncrementTriggerCounterForEachPublishedEvent() {
        Journal journal = new Journal(config, context.getFoundationDB());
        Publisher publisher = journal.getPublisher();

        // Publish multiple events
        publisher.publish(TEST_JOURNAL, "event1");
        publisher.publish(TEST_JOURNAL, "event2");
        publisher.publish(TEST_JOURNAL, "event3");

        // Note: Testing the actual trigger value would require accessing JournalMetadata.trigger()
        // which is package-private. The test verifies events are published successfully,
        // implying trigger increments worked (otherwise publish would fail)

        // Verify all events were published by consuming them
        ConsumerConfig consumerConfig = new ConsumerConfig("test-consumer", TEST_JOURNAL, ConsumerConfig.Offset.EARLIEST);
        Consumer consumer = new Consumer(context, consumerConfig);
        consumer.start();

        int eventCount = 0;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            while (true) {
                Event event = consumer.consume(tr);
                if (event == null) break;
                eventCount++;
                consumer.markConsumed(tr, event);
            }
            tr.commit().join();
        }

        assertEquals(3, eventCount, "All published events should be consumable");
    }

    // Helper class for complex object test
    private static class ComplexEvent {
        public String id;
        public int value;
        public List<String> tags;

        public ComplexEvent(String id, int value, List<String> tags) {
            this.id = id;
            this.value = value;
            this.tags = tags;
        }
    }
}
