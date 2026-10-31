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
 * Keeps the global prefix registry in the {@code PREFIXES} subspace. A prefix maps to its binding key,
 * and the binding key maps back to the prefix.
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
    public static void register(Context context, Transaction tr, byte[] prefixBindingKey, Prefix prefix) {
        byte[] prefixKey = packPrefixKey(context, prefix);
        byte[] existingPointer = tr.get(prefixKey).join();
        if (existingPointer != null && !Arrays.equals(existingPointer, prefixBindingKey)) {
            throw new PrefixCollisionException(
                    String.format("Prefix collision detected: prefix %d is already registered by a different pointer", prefix.asLong())
            );
        }

        tr.set(prefixBindingKey, prefix.asBytes());
        tr.set(prefixKey, prefixBindingKey);
    }

    /**
     * Returns true when the binding key of the prefix is gone or points to a different prefix.
     * A prefix that is not in the registry is not stale.
     */
    public static boolean isStale(Context context, Transaction tr, Prefix prefix) {
        byte[] prefixKey = packPrefixKey(context, prefix);
        byte[] prefixBindingKey = tr.get(prefixKey).join();
        if (prefixBindingKey == null) {
            // Not registered to global subspace. Possibly, this is an internal prefix.
            return false;
        }
        byte[] expectedRawPrefix = tr.get(prefixBindingKey).join();
        if (expectedRawPrefix == null) {
            return true;
        }
        return !Arrays.equals(prefix.asBytes(), expectedRawPrefix);
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
