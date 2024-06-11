/*
 * Copyright (c) 2023-2024 Kronotop
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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.redis.HashValue;
import com.kronotop.redis.StringValue;
import com.kronotop.redis.TransactionSizeLimitExceeded;
import com.kronotop.redis.storage.Shard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * The Persistence class is responsible for persisting data to the FoundationDB cluster.
 * It provides methods for persisting different types of data structures such as strings and hashes.
 */
public class Persistence {
    public static final int MAXIMUM_TRANSACTION_SIZE = 10_000_000;
    public static final String PERSISTENCE_LAYOUT_KEY = "persistence-layout";
    private static final Logger LOGGER = LoggerFactory.getLogger(Persistence.class);
    private final Context context;
    private final AtomicInteger transactionSize = new AtomicInteger();
    private final Shard shard;
    private final EnumMap<DataStructure, Node> layout = new EnumMap<>(DataStructure.class);

    /**
     * Constructs a Persistence object with the given context and shard.
     * Initializes the layout and directories for each data structure in the persistence layer.
     *
     * @param context the Context object containing information about the cluster and database
     * @param shard   the Shard object representing a shard in the cluster
     */
    public Persistence(Context context, Shard shard) {
        this.context = context;
        this.shard = shard;

        // TODO: Do we need this?
        ReadWriteLock readWriteLock = context.getStripedReadWriteLock().get(PERSISTENCE_LAYOUT_KEY);
        readWriteLock.writeLock().lock();
        try {
            for (DataStructure dataStructure : DataStructure.values()) {
                DirectorySubspace subspace = context.getDirectoryLayer().createOrOpenDataStructure(shard.getId(), dataStructure);
                layout.put(dataStructure, new Node(subspace));
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Checks if the persistence queue of a shard is empty.
     *
     * @return true if the persistence queue is empty, false otherwise
     */
    public boolean isQueueEmpty() {
        return shard.getPersistenceQueue().size() == 0;
    }

    /**
     * Persists the given string value with the specified key in the transaction.
     *
     * @param tr          the transaction object
     * @param key         the key object for the string value
     * @param stringValue the string value to be persisted
     * @throws IOException                  if an I/O error occurs while encoding the string value
     * @throws TransactionSizeLimitExceeded if the size of the transaction exceeds the maximum allowed size
     */
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

    /**
     * Persists the given hash value with the specified key in the transaction.
     *
     * @param tr        the transaction object
     * @param hashKey   the key object for the hash value
     * @param hashValue the hash value to be persisted
     * @throws TransactionSizeLimitExceeded if the size of the transaction exceeds the maximum allowed size
     */
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

    /**
     * Executes the persistence process for a batch of keys.
     * It retrieves a batch of keys from the persistence queue,
     * and for each key, it locks the corresponding lock, retrieves the latest value from the shard,
     * and persists the value in the transaction.
     * Finally, it commits the transaction and handles any exceptions that occur during the process.
     *
     * @throws RuntimeException if an exception occurs during the persistence process
     */
    public void run() {
        List<Key> keys = shard.getPersistenceQueue().poll(1000);
        if (keys.isEmpty()) {
            return;
        }
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (Key key : keys) {
                ReadWriteLock lock = shard.getStriped().get(key.getKey());
                lock.readLock().lock();
                try {
                    Object latestValue = shard.get(key.getKey());
                    if (key instanceof StringKey) {
                        persistStringValue(tr, (StringKey) key, (StringValue) latestValue);
                    } else if (key instanceof HashKey) {
                        persistHashValue(tr, (HashKey) key, (HashValue) latestValue);
                    } else {
                        LOGGER.warn("Unknown value type for key: {}", key.getKey());
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
                shard.getPersistenceQueue().add(key);
            }
            throw new RuntimeException(e);
        } finally {
            transactionSize.set(0);
        }
    }
}