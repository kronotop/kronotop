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

package com.kronotop;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.maintenance.IndexMaintenanceTask;
import com.kronotop.bucket.index.maintenance.IndexMaintenanceTaskKind;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

public class TestUtil {
    private static final Random random = new Random(System.nanoTime());

    public static Versionstamp generateVersionstamp(int userVersion) {
        byte[] trVersion = new byte[10];
        random.nextBytes(trVersion);
        return Versionstamp.complete(trVersion, userVersion);
    }

    public static Versionstamp findIndexMaintenanceTaskId(Context context, DirectorySubspace taskSubspace, IndexMaintenanceTaskKind kind) {
        AtomicReference<Versionstamp> taskId = new AtomicReference<>();
        await().atMost(15, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TaskStorage.tasks(tr, taskSubspace, (id) -> {
                    byte[] data = TaskStorage.getDefinition(tr, taskSubspace, id);
                    IndexMaintenanceTask task = JSONUtil.readValue(data, IndexMaintenanceTask.class);
                    if (task.getKind() == kind) {
                        // found the correct task
                        taskId.set(id);
                        return false; // break
                    }
                    return true;
                });
                return taskId.get() != null;
            }
        });
        return taskId.get();
    }

    public static List<KeyValue> fetchAllIndexedEntries(Context context, DirectorySubspace indexSubspace) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return tr.getRange(begin, end).asList().join();
        }
    }
}
