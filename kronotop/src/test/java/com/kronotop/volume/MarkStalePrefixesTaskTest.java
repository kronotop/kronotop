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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.kronotop.internal.DirectorySubspaceCache;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MarkStalePrefixesTaskTest extends BaseVolumeIntegrationTest {

    @Test
    void test_mark_stale_prefixes_task() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-subspace");
        byte[] prefixPointer = subspace.pack("prefix-pointer");
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < 20; i++) {
                Prefix testPrefix = new Prefix(UUID.randomUUID().toString());
                PrefixUtils.register(context, tr, prefixPointer, testPrefix);
            }
            tr.commit().join();
        }

        // Remove the pointer and make prefixes stale
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            tr.clear(prefixPointer);
            tr.commit().join();
        }

        // 20/5 = 4, it will take 4 iterations to remove all stale prefixes.
        MarkStalePrefixesTask task = new MarkStalePrefixesTask(context, 5);
        task.run();

        DirectorySubspace prefixesSubspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.PREFIXES);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] begin = prefixesSubspace.pack();
            byte[] end = ByteArrayUtil.strinc(begin);
            Range range = new Range(begin, end);
            List<KeyValue> prefixes = tr.getRange(range).asList().join();
            assertEquals(0, prefixes.size());
        }
    }
}