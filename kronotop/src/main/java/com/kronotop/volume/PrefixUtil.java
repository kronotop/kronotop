/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;
import com.kronotop.internal.DirectorySubspaceCache;

import java.util.Arrays;

/**
 * Keeps the global prefix registry in the {@code PREFIXES} subspace. A prefix maps to a tuple of its
 * binding key, data model kind and pointer. The binding key maps back to the prefix.
 */
public class PrefixUtil {

    private static byte[] packPrefixKey(Context context, Prefix prefix) {
        DirectorySubspace prefixesSubspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.PREFIXES);
        return prefixesSubspace.pack(Tuple.from((Object) prefix.asBytes()));
    }

    /**
     * Binds a prefix and its binding key to each other.
     *
     * @throws PrefixCollisionException if the prefix is already bound to a different key
     */
    public static void register(
            Context context,
            Transaction tr,
            Prefix prefix,
            PrefixEntry entry
    ) {
        byte[] prefixKey = packPrefixKey(context, prefix);
        byte[] existing = tr.get(prefixKey).join();
        if (existing != null && !Arrays.equals(PrefixEntry.decode(existing).key(), entry.key())) {
            throw new PrefixCollisionException(
                    String.format("Prefix collision detected: prefix %d is already registered by a different pointer", prefix.asLong())
            );
        }

        tr.set(entry.key(), prefix.asBytes());
        tr.set(prefixKey, entry.encode());
    }

    /**
     * Returns the registry entry of the prefix and whether it is stale. A prefix is stale when its
     * binding key is gone.
     *
     * @throws NoSuchPrefixException     if the prefix is not in the registry
     * @throws PrefixCorruptionException if the binding key points to a different prefix
     */
    public static PrefixInspection inspectPrefix(Context context, Transaction tr, Prefix prefix) {
        byte[] prefixKey = packPrefixKey(context, prefix);
        byte[] rawEntry = tr.get(prefixKey).join();
        if (rawEntry == null) {
            // Not registered to global subspace. Possibly, this is an internal prefix.
            throw new NoSuchPrefixException(
                    String.format("No such prefix: %d", prefix.asLong())
            );
        }
        PrefixEntry entry = PrefixEntry.decode(rawEntry);
        byte[] expectedRawPrefix = tr.get(entry.key()).join();
        if (expectedRawPrefix == null) {
            return new PrefixInspection(entry, true);
        }
        if (!Arrays.equals(prefix.asBytes(), expectedRawPrefix)) {
            throw new PrefixCorruptionException(
                    String.format("Prefix corruption detected: binding key of prefix %d points to a different prefix", prefix.asLong())
            );
        }
        return new PrefixInspection(entry, false);
    }

    /**
     * Removes the prefix and its binding key from the registry.
     */
    public static void unregister(Context context, Transaction tr, byte[] prefixBindingKey, Prefix prefix) {
        tr.clear(prefixBindingKey);
        byte[] prefixKey = packPrefixKey(context, prefix);
        tr.clear(prefixKey);
    }
}
