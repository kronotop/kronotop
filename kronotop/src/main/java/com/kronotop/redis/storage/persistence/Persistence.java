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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * The Persistence class is responsible for persisting data to the FoundationDB cluster.
 * It provides methods for persisting different types of data structures such as strings and hashes.
 */
public class Persistence {
    private static final Logger LOGGER = LoggerFactory.getLogger(Persistence.class);
    private final Context context;
    private final RedisShard shard;

    /**
     * Constructs a Persistence object with the given context and shard.
     * Initializes the layout and directories for each data structure in the persistence layer.
     *
     * @param context the Context object containing information about the cluster and database
     * @param shard   the Shard object representing a shard in the cluster
     */
    public Persistence(Context context, RedisShard shard) {
        this.context = context;
        this.shard = shard;
    }

    /**
     * Checks if the persistence queue of a shard is empty.
     *
     * @return true if the persistence queue is empty, false otherwise
     */
    public boolean isQueueEmpty() {
        return shard.persistenceQueue().size() == 0;
    }

    /**
     * Persist data for a list of keys.
     * <p>
     * This method takes a list of keys as input and persists the corresponding data to a storage system.
     * It uses a {@link PersistenceSession} object to pack the data structures into byte buffers,
     * which are then appended to the storage system.
     *
     * @param keys the list of keys to persist
     * @throws IOException if an I/O error occurs while persisting the data
     */
    private void persist(List<Key> keys) throws IOException {
        PersistenceSession session = new PersistenceSession(keys.size());
        List<Key> appendedKeys = new ArrayList<>();
        for (Key key : keys) {
            ReadWriteLock lock = shard.striped().get(key.data());
            lock.readLock().lock();
            try {
                RedisValueContainer container = shard.storage().get(key.data());
                if (container == null) {
                    // TODO: We need to find a way to remove keys from shard's volume
                    continue;
                }
                appendedKeys.add(key);
                switch (key.kind()) {
                    case STRING -> {
                        StringPack stringPack = new StringPack(key.data(), container.string());
                        session.pack(stringPack);
                    }
                    case HASH -> {
                        HashKey hashKey = (HashKey) key;
                        HashFieldPack hashFieldPack = new HashFieldPack(hashKey.data(), hashKey.getField(), container.hashField());
                        session.pack(hashFieldPack);
                    }
                    default -> LOGGER.warn("Unknown value type for key: {}", key.data());
                }
            } finally {
                lock.readLock().unlock();
            }
        }

        Versionstamp[] versionstampedKeys = session.persist();
        int index = 0;
        for (Versionstamp versionstamp : versionstampedKeys) {
            Key key = appendedKeys.get(index);
            ReadWriteLock lock = shard.striped().get(key.data());
            lock.writeLock().lock();
            try {
                RedisValueContainer container = shard.storage().get(key.data());
                if (container == null) {
                    // Deleted after persisting it to the shard's volume.
                    continue;
                }
                BaseRedisValue<?> value = container.baseRedisValue();
                value.setVersionstamp(versionstamp);
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    public void run() {
        List<Key> keys = shard.persistenceQueue().poll(1000);
        if (keys.isEmpty()) {
            return;
        }

        try {
            persist(keys);
        } catch (Exception e) {
            for (Key key : keys) {
                shard.persistenceQueue().add(key);
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * A PersistenceSession represents a session for persisting data to a storage system.
     * It provides methods for packing data structures into byte buffers and persisting them.
     */
    private class PersistenceSession {
        private final ByteBuffer[] entries;
        private int size;
        private int cursor;

        public PersistenceSession(int capacity) {
            this.entries = new ByteBuffer[capacity];
        }

        /**
         * Packs a data structure into the persistence session.
         * The data structure is packed into a {@link ByteBuffer} and added to the {@code entries} array.
         * The size of the packed buffer is added to the {@code size} field.
         * The cursor is incremented to the next position in the {@code entries} array.
         *
         * @param dataStructurePack the data structure to be packed
         */
        public void pack(DataStructurePack dataStructurePack) {
            ByteBuffer buffer = dataStructurePack.pack();
            size += buffer.remaining();
            entries[cursor++] = buffer;
        }

        /**
         * Persists the data stored in the PersistenceSession to a storage system.
         * The data is packed into byte buffers and appended to the storage system.
         *
         * @throws IOException if an I/O error occurs while persisting the data
         */
        public Versionstamp[] persist() throws IOException {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Session session = new Session(tr);
                AppendResult result = shard.volume().append(session, entries);
                tr.commit().join();

                Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("{} entries have persisted, total size is {}", versionstampedKeys.length, size());
                }

                return versionstampedKeys;
            }
        }

        public int size() {
            return size;
        }
    }
}