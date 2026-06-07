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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.namespace.handlers.Tombstone;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TombstoneManagerTest extends BaseStandaloneInstanceTest {

    private DirectorySubspace getTombstoneSubspace() {
        return context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.NAMESPACE_TOMBSTONES);
    }

    @Test
    void shouldSetTombstone() {
        // Behavior: setTombstone writes a UUID token under the NAMESPACE key and returns it.
        String token;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            token = TombstoneManager.setTombstone(context, tr, TEST_NAMESPACE);
            tr.commit().join();
        }

        assertNotNull(token);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace tombstones = getTombstoneSubspace();
            byte[] key = tombstones.pack(Tuple.from(Tombstone.NAMESPACE.getValue(), TEST_NAMESPACE));
            byte[] stored = tr.get(key).join();
            assertNotNull(stored);
            assertEquals(token, new String(stored, StandardCharsets.US_ASCII));
        }
    }

    @Test
    void shouldReturnDifferentTokensForEachCall() {
        // Behavior: Each setTombstone call generates a unique token.
        String token1;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            token1 = TombstoneManager.setTombstone(context, tr, TEST_NAMESPACE);
            tr.commit().join();
        }

        String token2;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            token2 = TombstoneManager.setTombstone(context, tr, TEST_NAMESPACE);
            tr.commit().join();
        }

        assertNotEquals(token1, token2);
    }

    @Test
    void shouldObserveTombstone() {
        // Behavior: observe writes the member's ID under the OBSERVERS key with the given token as value.
        String token;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            token = TombstoneManager.setTombstone(context, tr, TEST_NAMESPACE);
            TombstoneManager.observe(context, tr, TEST_NAMESPACE, token);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace tombstones = getTombstoneSubspace();
            byte[] key = tombstones.pack(
                    Tuple.from(Tombstone.OBSERVERS.getValue(), TEST_NAMESPACE, context.getMember().getId())
            );
            byte[] stored = tr.get(key).join();
            assertNotNull(stored);
            assertEquals(token, new String(stored, StandardCharsets.US_ASCII));
        }
    }

    @Test
    void shouldGetObserversWithMatchingToken() {
        // Behavior: getObservers returns member IDs whose observer entries match the given token.
        String token;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            token = TombstoneManager.setTombstone(context, tr, TEST_NAMESPACE);
            TombstoneManager.observe(context, tr, TEST_NAMESPACE, token);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> observers = TombstoneManager.getObservers(context, tr, TEST_NAMESPACE, token);
            assertEquals(1, observers.size());
            assertEquals(context.getMember().getId(), observers.get(0));
        }
    }

    @Test
    void shouldNotReturnObserversWithMismatchedToken() {
        // Behavior: getObservers filters out observer entries whose token doesn't match.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TombstoneManager.observe(context, tr, TEST_NAMESPACE, "token-a");
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> observers = TombstoneManager.getObservers(context, tr, TEST_NAMESPACE, "token-b");
            assertTrue(observers.isEmpty());
        }
    }

    @Test
    void shouldCleanupTombstoneAndObservers() {
        // Behavior: cleanup removes both the tombstone marker and all observer entries for the namespace.
        String token;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            token = TombstoneManager.setTombstone(context, tr, TEST_NAMESPACE);
            TombstoneManager.observe(context, tr, TEST_NAMESPACE, token);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TombstoneManager.cleanup(context, tr, TEST_NAMESPACE);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace tombstones = getTombstoneSubspace();
            byte[] tombstoneKey = tombstones.pack(Tuple.from(Tombstone.NAMESPACE.getValue(), TEST_NAMESPACE));
            assertNull(tr.get(tombstoneKey).join());

            List<String> observers = TombstoneManager.getObservers(context, tr, TEST_NAMESPACE, token);
            assertTrue(observers.isEmpty());
        }
    }

    @Test
    void shouldCleanupNonExistentNamespace() {
        // Behavior: cleanup on a namespace with no tombstone data completes without error.
        assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TombstoneManager.cleanup(context, tr, "non-existent-namespace");
                tr.commit().join();
            }
        });
    }

    @Test
    void shouldPassBarrierWhenNoTombstoneExists() {
        // Behavior: checkBarrier is a no-op when no tombstone exists for the namespace.
        String namespace = "no-tombstone-ns";
        assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TombstoneManager.checkBarrier(context, tr, namespace);
                tr.commit().join();
            }
        });

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertNull(TombstoneManager.getTombstone(context, tr, namespace));
        }
    }
}
