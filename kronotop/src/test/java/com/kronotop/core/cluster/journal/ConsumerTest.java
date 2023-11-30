/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.core.cluster.journal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerTest extends BaseJournalTest {
    private final String event = "{\"event\": \"foobar\"}";

    @Test
    public void testConsumeEvent() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String expectedEvent = objectMapper.writeValueAsString(event);

        Publisher publisher = new Publisher(context);
        long offset = publisher.publish(testJournal, event);

        Consumer consumer = new Consumer(context);
        Event consumeEvent = consumer.consumeEvent(testJournal, offset);
        assertEquals(expectedEvent, new String(consumeEvent.getValue()));
    }

    @Test
    public void testGetLatestIndex() {
        Publisher publisher = new Publisher(context);
        publisher.publish(testJournal, event);
        long expectedOffset = publisher.publish(testJournal, event);

        Consumer consumer = new Consumer(context);
        long offset = consumer.getLatestIndex(testJournal);
        assertEquals(expectedOffset, offset);
    }
}
