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

import static org.junit.jupiter.api.Assertions.assertTrue;

public class PublisherTest extends BaseStandaloneInstanceTest {
    private final String TEST_JOURNAL = "test-journal";

    @Test
    public void test_Publish() {
        Journal journal = new Journal(config, context.getFoundationDB());

        Publisher publisher = journal.getPublisher();
        VersionstampContainer first = publisher.publish(TEST_JOURNAL, "foo");
        VersionstampContainer second = publisher.publish(TEST_JOURNAL, "bar");

        Versionstamp firstVersionstamp = Versionstamp.complete(first.versionstamp().join(), first.userVersion());
        Versionstamp secondVersionstamp = Versionstamp.complete(second.versionstamp().join(), second.userVersion());
        assertTrue(firstVersionstamp.compareTo(secondVersionstamp) < 0);
    }

    @Test
    public void test_Publish_single_transaction() {
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
}
