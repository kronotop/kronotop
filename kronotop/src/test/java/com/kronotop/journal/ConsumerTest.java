/*
 * Copyright (c) 2023-2024 Kronotop
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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ConsumerTest extends BaseJournalTest {
    private final String event = "{\"event\": \"foobar\"}";

    @Test
    public void test_consumeByVersionstamp() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        byte[] expectedEvent = objectMapper.writeValueAsBytes(event);

        Journal journal = new Journal(config, database);
        Publisher publisher = journal.getPublisher();
        VersionstampContainer versionstampContainer = publisher.publish(testJournal, event);
        publisher.publish(testJournal, "1");
        publisher.publish(testJournal, "2");
        publisher.publish(testJournal, "3");

        Consumer consumer = journal.getConsumer();
        Event consumedEvent = consumer.consumeByVersionstamp(testJournal, versionstampContainer.complete());
        assertArrayEquals(expectedEvent, consumedEvent.getValue());
    }

    @Test
    public void test_getLatestEventKey() {
        Journal journal = new Journal(config, database);

        Publisher publisher = journal.getPublisher();

        VersionstampContainer first = publisher.publish(testJournal, "1");
        publisher.publish(testJournal, "2");
        publisher.publish(testJournal, "3");

        publisher.publish(testJournal, event);

        Consumer consumer = journal.getConsumer();
        try (Transaction tr = database.createTransaction()) {
            byte[] eventKey = consumer.getLatestEventKey(tr, testJournal);
            assertNotNull(eventKey);

            Versionstamp versionstamp = consumer.getVersionstampFromKey(testJournal, eventKey);
            assertTrue(versionstamp.compareTo(first.complete()) > 0);
        }
    }

    @Test
    public void test_consumeNext() {
        Journal journal = new Journal(config, database);
        Publisher publisher = journal.getPublisher();

        Versionstamp first = publisher.publish(testJournal, "1").complete();
        publisher.publish(testJournal, "2").complete();
        publisher.publish(testJournal, event);

        String expectedValue = "\"2\"";
        Consumer consumer = journal.getConsumer();
        try (Transaction tr = database.createTransaction()) {
            Event firstEvent = consumer.consumeByVersionstamp(tr, testJournal, first);
            assertNotNull(firstEvent);
            Event nextEvent = consumer.consumeNext(tr, testJournal, firstEvent.getKey());
            assertArrayEquals(expectedValue.getBytes(), nextEvent.getValue());
        }
    }

    @Test
    public void test_consumeNext_null_key() throws JsonProcessingException {
        Journal journal = new Journal(config, database);
        Publisher publisher = journal.getPublisher();

        publisher.publish(testJournal, event);
        publisher.publish(testJournal, "foo");
        publisher.publish(testJournal, "bar");

        ObjectMapper objectMapper = new ObjectMapper();
        byte[] expectedEvent = objectMapper.writeValueAsBytes(event);

        Consumer consumer = journal.getConsumer();
        try (Transaction tr = database.createTransaction()) {
            Event consumed = consumer.consumeNext(tr, testJournal, null);
            assertNotNull(consumed);
            assertArrayEquals(expectedEvent, consumed.getValue());
        }
    }
}
