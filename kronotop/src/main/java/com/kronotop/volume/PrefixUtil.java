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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;
import com.kronotop.internal.DirectorySubspaceCache;

import java.util.Arrays;

public class PrefixUtil {

    /**
     * Registers a prefix in the context's DirectorySubspace, associating it with a given prefix pointer
     * and storing the mapping in the `PREFIXES` subspace.
     *
     * @param context       the current context of the Kronotop instance managing DirectorySubspaceCache
     * @param tr            the active transaction used to register the prefix and pointer
     * @param prefixPointer the byte array that serves as a pointer to the prefix being registered
     * @param prefix        the prefix object whose bytes are to be associated in the DirectorySubspace
     */
    public static void register(Context context, Transaction tr, byte[] prefixPointer, Prefix prefix) {
        tr.set(prefixPointer, prefix.asBytes());

        DirectorySubspace prefixesSubspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.PREFIXES);
        byte[] prefixKey = prefixesSubspace.pack(Tuple.from((Object) prefix.asBytes()));
        tr.set(prefixKey, prefixPointer);
    }

    /**
     * Determines if a given prefix is stale in the context of a transaction.
     * A prefix is considered stale if its pointer no longer points to its expected raw prefix
     * or if it is not registered correctly in the global subspace.
     *
     * @param context the current context of the Kronotop instance managing DirectorySubspaceCache
     * @param tr      the active transaction used to check the prefix state
     * @param prefix  the prefix object to evaluate for staleness
     * @return true if the prefix is stale, false otherwise
     */
    public static boolean isStale(Context context, Transaction tr, Prefix prefix) {
        DirectorySubspace prefixesSubspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.PREFIXES);
        byte[] prefixKey = prefixesSubspace.pack(Tuple.from((Object) prefix.asBytes()));

        byte[] prefixPointer = tr.get(prefixKey).join();
        if (prefixPointer == null) {
            // Not registered to global subspace. Possibly, this is an internal prefix.
            return false;
        }
        byte[] expectedRawPrefix = tr.get(prefixPointer).join();
        if (expectedRawPrefix == null) {
            return true;
        }
        return !Arrays.equals(prefix.asBytes(), expectedRawPrefix);
    }

    /**
     * Unregisters a prefix from the context's DirectorySubspace, removing its association
     * with both the prefix pointer and the key in the `PREFIXES` subspace.
     *
     * @param context       the current context of the Kronotop instance used to manage DirectorySubspaceCache
     * @param tr            the active transaction during which the prefix and pointer are unregistered
     * @param prefixPointer the byte array that serves as a pointer to the prefix being unregistered
     * @param prefix        the prefix object whose bytes are to be removed from the DirectorySubspace
     */
    public static void unregister(Context context, Transaction tr, byte[] prefixPointer, Prefix prefix) {
        tr.clear(prefixPointer);

        DirectorySubspace prefixesSubspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.PREFIXES);
        byte[] prefixKey = prefixesSubspace.pack(Tuple.from((Object) prefix.asBytes()));
        tr.clear(prefixKey);
    }
}
