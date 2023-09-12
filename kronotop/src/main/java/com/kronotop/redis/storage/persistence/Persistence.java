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

package com.kronotop.redis.storage.persistence;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;
import com.kronotop.redis.HashValue;
import com.kronotop.redis.StringValue;
import com.kronotop.redis.TransactionSizeLimitExceeded;
import com.kronotop.redis.storage.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

public class Persistence {
    public static final int MAXIMUM_TRANSACTION_SIZE = 10_000_000;
    private static final Logger logger = LoggerFactory.getLogger(Persistence.class);
    private final Context context;
    private final AtomicInteger transactionSize = new AtomicInteger();
    private final Partition partition;
    private final EnumMap<DataStructure, Node> layout = new EnumMap<>(DataStructure.class);

    public Persistence(Context context, String logicalDatabase, Partition partition) {
        this.context = context;
        this.partition = partition;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (DataStructure dataStructure : DataStructure.values()) {
                List<String> subpath = DirectoryLayout.Builder.
                        clusterName(context.getClusterName()).
                        internal().
                        redis().
                        persistence().
                        logicalDatabase(logicalDatabase).
                        partitionId(partition.getId().toString()).
                        dataStructure(dataStructure.name().toLowerCase()).
                        asList();
                DirectorySubspace subspace = DirectoryLayer.getDefault().createOrOpen(tr, subpath).join();
                layout.put(dataStructure, new Node(subspace));
            }
            tr.commit().join();
        }
    }

    public boolean isQueueEmpty() {
        return partition.getPersistenceQueue().size() == 0;
    }

    private void persistStringValue(Transaction tr, StringKey key, StringValue stringValue) throws IOException {
        Node node = layout.get(DataStructure.STRING);
        byte[] packetKey = node.getSubspace().pack(key.getKey());
        if (stringValue == null) {
            tr.clear(packetKey);
            return;
        }
        byte[] data = stringValue.encode();
        if (data.length + transactionSize.get() >= MAXIMUM_TRANSACTION_SIZE) {
            throw new TransactionSizeLimitExceeded();
        }
        tr.set(packetKey, data);
        transactionSize.addAndGet(data.length);
    }

    private void persistHashValue(Transaction tr, HashKey hashKey, HashValue hashValue) {
        Node node = layout.get(DataStructure.HASH);
        DirectorySubspace subspace = node.getLeaf(tr.getDatabase(), hashKey.getKey()).getSubspace();
        byte[] packedKey = subspace.pack(hashKey.getField());
        if (hashValue == null) {
            tr.clear(packedKey);
            return;
        }

        byte[] value = hashValue.get(hashKey.getField());
        if (value != null) {
            if (value.length + transactionSize.get() >= MAXIMUM_TRANSACTION_SIZE) {
                throw new TransactionSizeLimitExceeded();
            }
            tr.set(packedKey, value);
        } else {
            tr.clear(packedKey);
        }
    }

    public void run() {
        List<Key> keys = partition.getPersistenceQueue().poll(1000);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (Key key : keys) {
                ReadWriteLock lock = partition.getStriped().get(key.getKey());
                lock.readLock().lock();
                try {
                    Object latestValue = partition.get(key.getKey());
                    if (key instanceof StringKey) {
                        persistStringValue(tr, (StringKey) key, (StringValue) latestValue);
                    } else if (key instanceof HashKey) {
                        persistHashValue(tr, (HashKey) key, (HashValue) latestValue);
                    } else {
                        logger.warn("Unknown value type for key: {}", key.getKey());
                    }
                } catch (TransactionSizeLimitExceeded e) {
                    break;
                } finally {
                    lock.readLock().unlock();
                }
            }
            tr.commit().join();
        } catch (Exception e) {
            for (Key key : keys) {
                partition.getPersistenceQueue().add(key);
            }
            throw new RuntimeException(e);
        } finally {
            transactionSize.set(0);
        }
    }
}