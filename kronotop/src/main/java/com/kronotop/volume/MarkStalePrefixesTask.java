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
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.journal.JournalName;
import com.kronotop.task.BaseTask;
import com.kronotop.task.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class MarkStalePrefixesTask extends BaseTask implements Task {
    public static final String NAME = "volume:mark-stale-prefixes-task";
    private static final Logger LOGGER = LoggerFactory.getLogger(MarkStalePrefixesTask.class);
    private final int batchSize;
    private final Context context;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final DirectorySubspace subspace;
    private volatile boolean shutdown;

    public MarkStalePrefixesTask(Context context) {
        this(context, 10000);
    }

    protected MarkStalePrefixesTask(Context context, int batchSize) {
        // Intended for tests
        this.context = context;
        this.batchSize = batchSize;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            this.subspace = openTaskSubspace(tr);
            setMemberId(tr);
            tr.commit().join();
        }
    }

    private void setMemberId(Transaction tr) {
        byte[] value = tr.get(subspace.pack(METADATA_KEY.MEMBER_ID.name())).join();
        if (value == null) {
            tr.set(subspace.pack(METADATA_KEY.MEMBER_ID.name()), context.getMember().getId().getBytes());
        } else {
            String memberId = new String(value);
            if (!context.getMember().getId().equals(memberId)) {
                throw new KronotopException("Run by another cluster member");
            }
        }
    }

    private DirectorySubspace openTaskSubspace(Transaction tr) {
        KronotopDirectoryNode node = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                tasks().
                task(NAME);
        return DirectoryLayer.getDefault().createOrOpen(tr, node.toList()).join();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean isCompleted() {
        return latch.getCount() == 0;
    }

    public void complete() {
        shutdown();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            KronotopDirectoryNode node = KronotopDirectory.
                    kronotop().
                    cluster(context.getClusterName()).
                    metadata().
                    tasks().
                    task(NAME);
            DirectoryLayer.getDefault().removeIfExists(tr, node.toList()).join();
            tr.commit().join();
        }
        // removed from FDB, mark it as completed.
        latch.countDown();
        LOGGER.info("{} task has been completed", NAME);
    }

    @Override
    public void task() {
        // Blocking call
        DirectorySubspace prefixesSubspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.PREFIXES);
        while (!shutdown) {
            int total = 0;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                byte[] begin = tr.get(subspace.pack(METADATA_KEY.LAST_PREFIX.name())).join();
                byte[] end = ByteArrayUtil.strinc(prefixesSubspace.pack());
                if (begin == null) {
                    begin = prefixesSubspace.pack();
                }

                Range range = new Range(begin, end);
                byte[] latestKey = null;
                AsyncIterable<KeyValue> iterator = tr.getRange(range, batchSize);
                for (KeyValue keyValue : iterator) {
                    if (shutdown) {
                        break;
                    }
                    byte[] prefix = (byte[]) prefixesSubspace.unpack(keyValue.getKey()).get(0);
                    if (PrefixUtils.isStale(context, tr, Prefix.fromBytes(prefix))) {
                        tr.clear(keyValue.getKey());
                        context.getJournal().getPublisher().publish(tr, JournalName.DISUSED_PREFIXES, prefix);
                    }
                    latestKey = keyValue.getKey();
                    total++;
                }
                if (latestKey != null) {
                    tr.set(subspace.pack(METADATA_KEY.LAST_PREFIX.name()), latestKey);
                }
                tr.commit().join();
            }

            if (total == 0) {
                complete();
            }
        }
    }

    @Override
    public void shutdown() {
        if (shutdown) {
            return;
        }
        shutdown = true;
    }

    @Override
    public void awaitCompletion() throws InterruptedException {
        latch.await();
    }

    protected enum METADATA_KEY {
        MEMBER_ID,
        LAST_PREFIX
    }
}
