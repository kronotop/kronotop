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

import com.apple.foundationdb.Transaction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PublisherTest extends BaseJournalTest {

    @Test
    public void testPublish() {
        Publisher publisher = new Publisher(context);

        assertEquals(1, publisher.publish(testJournal, "foo"));
        assertEquals(2, publisher.publish(testJournal, "foo"));
    }

    @Test
    public void testPublish_single_transaction() {
        Publisher publisher = new Publisher(context);

        try (Transaction tr = database.createTransaction()) {
            assertEquals(1, publisher.publish(tr, testJournal, "foo"));
            assertEquals(2, publisher.publish(tr, testJournal, "foo"));
            tr.commit().join();
        }
    }
}
