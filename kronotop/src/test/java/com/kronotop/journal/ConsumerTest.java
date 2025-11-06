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
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.internal.JSONUtil;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerTest extends BaseStandaloneInstanceTest {
    private final String CONSUMER_ID = "consumer-id";
    private final String JOURNAL_NAME = "test-journal";
    private final List<String> EVENTS = new LinkedList<>(List.of(
            "{\"key1\": \"value1\"}",
            "{\"key2\": \"value2\"}",
            "{\"key3\": \"value3\"}"
    ));

    @Test
    void shouldConsumeAllEventsWhenOffsetIsEarliest() {
        Journal journal = new Journal(config, context.getFoundationDB());
        Publisher publisher = journal.getPublisher();

        List<String> expectedEvents = new LinkedList<>();
        for (String event : EVENTS) {
            publisher.publish(JOURNAL_NAME, event).complete();
            // publisher encodes the event into JSON.
            expectedEvents.add(new String(JSONUtil.writeValueAsBytes(event)));
        }

        ConsumerConfig config = new ConsumerConfig(CONSUMER_ID, JOURNAL_NAME, ConsumerConfig.Offset.EARLIEST);
        Consumer consumer = new Consumer(context, config);
        consumer.start();

        List<String> consumedEvents = new LinkedList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            while (true) {
                Event event = consumer.consume(tr);
                if (event == null) {
                    break;
                }
                consumer.markConsumed(tr, event);
                consumedEvents.add(new String(event.value()));
            }
            tr.commit().join();
        }
        assertEquals(consumedEvents, expectedEvents);
    }

    @Test
    void shouldConsumeNothingWhenOffsetIsLatestAndEventsAlreadyPublished() {
        Journal journal = new Journal(config, context.getFoundationDB());
        Publisher publisher = journal.getPublisher();

        for (String event : EVENTS) {
            publisher.publish(JOURNAL_NAME, event).complete();
        }

        ConsumerConfig config = new ConsumerConfig(CONSUMER_ID, JOURNAL_NAME, ConsumerConfig.Offset.LATEST);
        Consumer consumer = new Consumer(context, config);
        consumer.start();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertNull(consumer.consume(tr));
        }
    }

    @Test
    void shouldConsumeNewEventsWhenOffsetIsLatestAndConsumerStartedFirst() {
        Journal journal = new Journal(config, context.getFoundationDB());
        Publisher publisher = journal.getPublisher();

        ConsumerConfig config = new ConsumerConfig(CONSUMER_ID, JOURNAL_NAME, ConsumerConfig.Offset.LATEST);
        Consumer consumer = new Consumer(context, config);
        consumer.start();

        List<String> expectedEvents = new LinkedList<>();
        for (String event : EVENTS) {
            publisher.publish(JOURNAL_NAME, event).complete();
            // publisher encodes the event into JSON.
            expectedEvents.add(new String(JSONUtil.writeValueAsBytes(event)));
        }

        List<String> consumedEvents = new LinkedList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            while (true) {
                Event event = consumer.consume(tr);
                if (event == null) {
                    break;
                }
                consumer.markConsumed(tr, event);
                consumedEvents.add(new String(event.value()));
            }
            tr.commit().join();
        }
        assertEquals(expectedEvents, consumedEvents);
    }

    @Test
    void shouldResumeFromLastCommittedOffsetWhenOffsetIsResume() {
        Journal journal = new Journal(config, context.getFoundationDB());
        Publisher publisher = journal.getPublisher();

        String firstEvent = "first-message";
        publisher.publish(JOURNAL_NAME, firstEvent).complete();

        ConsumerConfig config = new ConsumerConfig(CONSUMER_ID, JOURNAL_NAME, ConsumerConfig.Offset.RESUME);
        Consumer firstConsumer = new Consumer(context, config);
        firstConsumer.start();

        // Consume the first event
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Event event = firstConsumer.consume(tr);
            firstConsumer.markConsumed(tr, event);
            tr.commit().join();
        }

        String secondEvent = "second-message";
        publisher.publish(JOURNAL_NAME, secondEvent).complete();

        // Assume that the first consumer has been shut downed and we created a second one.
        Consumer secondConsumer = new Consumer(context, config);
        secondConsumer.start();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Event event = secondConsumer.consume(tr);
            assertEquals(secondEvent, JSONUtil.readValue(event.value(), String.class));
        }
    }

    @Test
    void shouldConsumeSameEventWhenNotMarkedAsConsumed() {
        Journal journal = new Journal(config, context.getFoundationDB());
        Publisher publisher = journal.getPublisher();

        String firstEvent = "first-message";
        publisher.publish(JOURNAL_NAME, firstEvent).complete();

        ConsumerConfig config = new ConsumerConfig(CONSUMER_ID, JOURNAL_NAME, ConsumerConfig.Offset.EARLIEST);
        Consumer firstConsumer = new Consumer(context, config);
        firstConsumer.start();

        // Consume the first event
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Event event = firstConsumer.consume(tr);
            assertEquals(firstEvent, JSONUtil.readValue(event.value(), String.class));
        }

        Consumer secondConsumer = new Consumer(context, config);
        secondConsumer.start();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Event event = secondConsumer.consume(tr);
            assertEquals(firstEvent, JSONUtil.readValue(event.value(), String.class));
        }
    }

    @Test
    void shouldThrowExceptionWhenConsumeCalledBeforeStarting() {
        ConsumerConfig config = new ConsumerConfig(CONSUMER_ID, JOURNAL_NAME, ConsumerConfig.Offset.EARLIEST);
        Consumer consumer = new Consumer(context, config);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertThrows(IllegalStateException.class, () -> consumer.consume(tr));
        }
    }

    @Test
    void shouldThrowExceptionWhenConsumeCalledAfterStopping() {
        ConsumerConfig config = new ConsumerConfig(CONSUMER_ID, JOURNAL_NAME, ConsumerConfig.Offset.EARLIEST);
        Consumer consumer = new Consumer(context, config);
        consumer.start();
        consumer.stop();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertThrows(IllegalStateException.class, () -> consumer.consume(tr));
        }
    }

    @Test
    void shouldRewindOffsetWhenTransactionFails() {
        Journal journal = new Journal(config, context.getFoundationDB());
        Publisher publisher = journal.getPublisher();

        // Publish two events
        String firstEvent = "first-message";
        String secondEvent = "second-message";
        publisher.publish(JOURNAL_NAME, firstEvent).complete();
        publisher.publish(JOURNAL_NAME, secondEvent).complete();

        ConsumerConfig config = new ConsumerConfig(CONSUMER_ID, JOURNAL_NAME, ConsumerConfig.Offset.EARLIEST);
        Consumer consumer = new Consumer(context, config);
        consumer.start();

        // First transaction: consume and mark, but DON'T commit (simulates failure)
        Event event1;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            event1 = consumer.consume(tr);
            assertNotNull(event1);
            assertEquals(firstEvent, JSONUtil.readValue(event1.value(), String.class));
            consumer.markConsumed(tr, event1);
            // Intentionally NOT calling tr.commit() - simulates transaction failure
        }

        // Second transaction: should rewind and consume the same event again
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Event event2 = consumer.consume(tr);
            assertNotNull(event2);
            // Should be the SAME event as before (rewound to checkpoint)
            assertEquals(firstEvent, JSONUtil.readValue(event2.value(), String.class));
            assertArrayEquals(event1.key(), event2.key());

            // Now commit successfully
            consumer.markConsumed(tr, event2);
            tr.commit().join();
        }

        // Third transaction: should now move forward to second event
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Event event3 = consumer.consume(tr);
            assertNotNull(event3);
            assertEquals(secondEvent, JSONUtil.readValue(event3.value(), String.class));
        }
    }
}