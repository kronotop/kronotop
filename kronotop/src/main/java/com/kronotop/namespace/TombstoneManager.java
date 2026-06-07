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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.BarrierNotSatisfiedException;
import com.kronotop.Context;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MemberView;
import com.kronotop.cluster.MembershipService;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.journal.JournalName;
import com.kronotop.namespace.handlers.NamespaceMovedEvent;
import com.kronotop.namespace.handlers.Tombstone;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Manages tombstones for moved namespaces. When a namespace is moved (renamed), a tombstone with a
 * unique token is written under the old name. Every alive cluster member must observe the tombstone
 * before the old name can be reused for a new namespace. This prevents stale references across the cluster.
 */
public class TombstoneManager {

    /**
     * Sets a tombstone for the given namespace with a randomly generated token.
     *
     * @return the generated token
     */
    public static String setTombstone(Context context, Transaction tr, String namespace) {
        DirectorySubspace tombstones = context.getDirectorySubspaceCache().
                get(DirectorySubspaceCache.Key.NAMESPACE_TOMBSTONES);
        byte[] key = tombstones.pack(Tuple.from(Tombstone.NAMESPACE.getValue(), namespace));
        String token = UUID.randomUUID().toString();
        tr.set(key, token.getBytes(StandardCharsets.US_ASCII));
        return token;
    }

    /**
     * Returns the tombstone token for the given namespace, or {@code null} if no tombstone exists.
     */
    public static String getTombstone(Context context, Transaction tr, String namespace) {
        DirectorySubspace tombstones = context.getDirectorySubspaceCache().
                get(DirectorySubspaceCache.Key.NAMESPACE_TOMBSTONES);
        byte[] key = tombstones.pack(Tuple.from(Tombstone.NAMESPACE.getValue(), namespace));
        byte[] value = tr.get(key).join();
        if (value == null) {
            return null;
        }
        return new String(value);
    }

    /**
     * Records the current member as an observer of the tombstone for the given namespace.
     */
    public static void observe(Context context, Transaction tr, String namespace, String token) {
        DirectorySubspace tombstones = context.getDirectorySubspaceCache().
                get(DirectorySubspaceCache.Key.NAMESPACE_TOMBSTONES);
        byte[] key = tombstones.pack(
                Tuple.from(Tombstone.OBSERVERS.value, namespace, context.getMember().getId())
        );
        tr.set(key, token.getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * Removes the tombstone and all observer entries for the given namespace.
     */
    public static void cleanup(Context context, Transaction tr, String namespace) {
        DirectorySubspace tombstones = context.getDirectorySubspaceCache().
                get(DirectorySubspaceCache.Key.NAMESPACE_TOMBSTONES);
        tr.clear(tombstones.pack(Tuple.from(Tombstone.NAMESPACE.value, namespace)));
        tr.clear(Range.startsWith(tombstones.pack(Tuple.from(Tombstone.OBSERVERS.getValue(), namespace))));
    }

    /**
     * Returns the member IDs that have observed the tombstone matching the given token.
     */
    public static List<String> getObservers(Context context, Transaction tr, String namespace, String token) {
        DirectorySubspace tombstones = context.getDirectorySubspaceCache().
                get(DirectorySubspaceCache.Key.NAMESPACE_TOMBSTONES);
        byte[] prefix = tombstones.pack(Tuple.from(Tombstone.OBSERVERS.getValue(), namespace));

        List<String> observers = new ArrayList<>();
        for (KeyValue keyValue : tr.getRange(Range.startsWith(prefix))) {
            String storedToken = new String(keyValue.getValue(), StandardCharsets.US_ASCII);
            if (storedToken.equals(token)) {
                Tuple keyTuple = tombstones.unpack(keyValue.getKey());
                String memberId = keyTuple.getString(2);
                observers.add(memberId);
            }
        }
        return observers;
    }

    /**
     * Verifies that all alive cluster members have observed the tombstone for the given namespace.
     * If the barrier is satisfied, the tombstone is cleaned up. If any alive member has not yet
     * observed, a {@link BarrierNotSatisfiedException} is thrown and the event is re-published
     * to nudge lagging members.
     *
     * @throws BarrierNotSatisfiedException if any alive member has not observed the tombstone
     */
    public static void checkBarrier(Context context, Transaction tr, String namespace) {
        String token = TombstoneManager.getTombstone(context, tr, namespace);
        if (token != null) {
            List<String> observers = TombstoneManager.getObservers(context, tr, namespace, token);
            MembershipService membership = context.getService(MembershipService.NAME);
            for (Map.Entry<Member, MemberView> entry : membership.getKnownMembers().entrySet()) {
                if (entry.getValue().isAlive() && !observers.contains(entry.getKey().getId())) {
                    // Trigger the cluster again, the following method uses its own transaction.
                    context.getJournal().getPublisher().publish(
                            JournalName.NAMESPACE_EVENTS,
                            new NamespaceMovedEvent(namespace, token)
                    );
                    throw new BarrierNotSatisfiedException(
                            "Not all cluster members have observed the tombstone for namespace '" + namespace + "'"
                    );
                }
            }
            TombstoneManager.cleanup(context, tr, namespace);
        }
    }
}
