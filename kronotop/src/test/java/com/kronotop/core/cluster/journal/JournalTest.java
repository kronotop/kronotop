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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.ConfigTestUtil;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;
import com.kronotop.core.ContextImpl;
import com.kronotop.core.FoundationDBFactory;
import com.kronotop.core.cluster.ClusterService;
import com.kronotop.core.cluster.Member;
import com.kronotop.core.network.Address;
import com.typesafe.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;

public class JournalTest {
    private final String broadcastKey = "journal-test";
    protected Config config;
    protected Database database;
    protected Context context;

    @BeforeEach
    public void setup() {
        config = ConfigTestUtil.load("test.conf");
        database = FoundationDBFactory.newDatabase(config);
        try {
            Address address = new Address("localhost", 0);
            Member member = new Member(address, Instant.now().toEpochMilli());
            context = new ContextImpl(config, member, database);
        } catch (UnknownHostException e) {
            throw new KronotopException(e);
        }
        new ClusterService(context);
    }

    @AfterEach
    public void teardown() {
        database.run(tr -> {
            DirectoryLayer directoryLayer = DirectoryLayer.getDefault();
            List<String> subpath = DirectoryLayout.Builder.clusterName(context.getClusterName()).asList();
            return directoryLayer.remove(tr, subpath).join();
        });
        database.close();
    }


    @Test
    public void testPublish() {
        Journal journal = new Journal(context, broadcastKey);

        long offsetOne = journal.publish("value-1".getBytes());
        assertEquals(1L, offsetOne);

        long offsetTwo = journal.publish("value-1".getBytes());
        assertEquals(2L, offsetTwo);
    }

    @Test
    public void testConsume_FromSpecificOffset() {
        Journal journal = new Journal(context, broadcastKey);

        for (int i = 1; i <= 5; i++) {
            journal.publish(String.format("value-%d", i).getBytes());
        }

        JournalItem journalItem = journal.consume(3);
        assertEquals(3, journalItem.getOffset());
        assertEquals("value-3", new String(journalItem.getValue()));
    }

    @Test
    public void testConsume_StartFromBeginning() {
        Journal journal = new Journal(context, broadcastKey);
        for (int i = 1; i <= 5; i++) {
            journal.publish(String.format("value-%d", i).getBytes());
        }

        for (int i = 1; i <= 5; i++) {
            JournalItem journalItem = journal.consume(i);
            assertEquals(i, journalItem.getOffset());
            assertEquals(String.format("value-%d", i), new String(journalItem.getValue()));
        }
    }

    @Test
    public void testGetLastIndex() {
        Journal journal = new Journal(context, broadcastKey);
        for (int i = 1; i <= 5; i++) {
            journal.publish(String.format("value-%d", i).getBytes());
        }
        assertTrue(journal.getLastIndex() > 0);
    }

    @Test
    public void testConsume_IllegalOffset() {
        Journal journal = new Journal(context, broadcastKey);
        for (int i = 1; i <= 5; i++) {
            journal.publish(String.format("value-%d", i).getBytes());
        }

        CompletionException exception = assertThrows(CompletionException.class, () -> journal.consume(0));
        assertTrue(exception.getCause().getMessage().contentEquals("0 is an illegal offset"));
    }
}
