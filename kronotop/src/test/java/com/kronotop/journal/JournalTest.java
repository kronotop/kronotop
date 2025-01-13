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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JournalTest extends BaseJournalTest {

    @Test
    public void test_listJournals() {
        Journal journal = new Journal(config, database);

        try (Transaction tr = database.createTransaction()) {
            journal.getPublisher().publish(tr, testJournal, "foo");
            tr.commit().join();
        }

        List<String> journals = journal.listJournals();
        assertEquals(1, journals.size());
        assertEquals(testJournal, journals.getFirst());
    }
}
