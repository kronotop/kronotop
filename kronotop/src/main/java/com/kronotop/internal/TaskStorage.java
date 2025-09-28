/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.internal;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class TaskStorage {
    private static final byte DEFINITION = 0x44;
    private static final byte STATE = 0x53;

    public static Versionstamp create(Context context, DirectorySubspace subspace, byte[] definition) {
        byte[] key = subspace.packWithVersionstamp(Tuple.from(Versionstamp.incomplete(), DEFINITION));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            CompletableFuture<byte[]> future = tr.getVersionstamp();
            tr.set(key, definition);
            tr.commit().join();
            byte[] trVersion = future.join();
            return Versionstamp.complete(trVersion);
        }
    }

    public static byte[] getDefinition(Transaction tr, DirectorySubspace subspace, Versionstamp taskId) {
        byte[] key = subspace.pack(Tuple.from(taskId, DEFINITION));
        return tr.get(key).join();
    }

    public static void setStateField(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, String field, byte[] value) {
        byte[] key = subspace.pack(Tuple.from(taskId, STATE, field));
        tr.set(key, value);
    }

    public static byte[] getStateField(Transaction tr, DirectorySubspace subspace, Versionstamp taskId, String field) {
        byte[] key = subspace.pack(Tuple.from(taskId, STATE, field));
        return tr.get(key).join();
    }

    public static Map<String, byte[]> getStateFields(Transaction tr, DirectorySubspace subspace, Versionstamp taskId) {
        byte[] begin = subspace.pack(Tuple.from(taskId, STATE));
        byte[] end = ByteArrayUtil.strinc(begin);
        Map<String, byte[]> entries = new HashMap<>();
        for (KeyValue entry : tr.getRange(begin, end)) {
            Tuple tuple = subspace.unpack(entry.getKey());
            String key = tuple.get(2).toString();
            entries.put(key, entry.getValue());
        }
        return entries;
    }
}
