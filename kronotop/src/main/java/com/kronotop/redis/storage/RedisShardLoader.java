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

package com.kronotop.redis.storage;

import com.apple.foundationdb.Transaction;
import com.kronotop.Context;
import com.kronotop.VersionstampUtils;
import com.kronotop.redis.handlers.hash.HashValue;
import com.kronotop.volume.KeyEntry;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * The RedisShardLoader class is responsible for loading and processing data from a RedisShard's
 * storage volume into its operational state. It interacts with a Context and RedisShard to read,
 * process, and update data structures stored in the shard.
 */
public final class RedisShardLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisShardLoader.class);
    private final Context context;
    private final RedisShard shard;
    private final Prefix prefix;

    public RedisShardLoader(Context context, RedisShard shard) {
        this.context = context;
        this.shard = shard;
        this.prefix = new Prefix(context.getConfig().getString("redis.volume_syncer.prefix").getBytes());
    }

    /**
     * Processes a StringPack object based on the provided KeyEntry. The method unpacks the given
     * KeyEntry to retrieve StringPack data, acquires a write lock corresponding to the StringPack key,
     * and stores the StringPack contents into the shard's storage. The StringPack's versionstamp is
     * updated to match the KeyEntry's versionstamp.
     *
     * @param entry the KeyEntry containing the data to be unpacked and processed
     * @throws IOException if the KeyEntry cannot be unpacked or contains invalid data
     */
    public static void processStringPack(RedisShard shard, KeyEntry entry) throws IOException {
        StringPack pack = StringPack.unpack(entry.entry());
        ReadWriteLock lock = shard.striped().get(pack.key());
        lock.writeLock().lock();
        try {
            RedisValueContainer container = new RedisValueContainer(pack.stringValue());
            container.string().setVersionstamp(entry.key());
            shard.storage().put(pack.key(), container);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Processes a HashFieldPack object based on the provided KeyEntry. The method unpacks the given
     * KeyEntry to retrieve HashFieldPack data, acquires a write lock corresponding to the HashFieldPack
     * key, and stores the HashFieldPack contents into the shard's storage. The HashFieldPack's versionstamp
     * is updated to match the KeyEntry's versionstamp.
     *
     * @param entry the KeyEntry containing the data to be unpacked and processed
     * @throws IOException if the KeyEntry cannot be unpacked or contains invalid data
     */
    public static void processHashFieldPack(RedisShard shard, KeyEntry entry) throws IOException {
        HashFieldPack pack = HashFieldPack.unpack(entry.entry());
        ReadWriteLock lock = shard.striped().get(pack.key());
        lock.writeLock().lock();
        try {
            RedisValueContainer container = shard.storage().get(pack.key());
            if (container == null) {
                container = new RedisValueContainer(new HashValue());
                shard.storage().put(pack.key(), container);
            }
            pack.hashFieldValue().setVersionstamp(entry.key());
            container.hash().put(pack.field(), pack.hashFieldValue());
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void processStringPack(KeyEntry entry) throws IOException {
        processStringPack(shard, entry);
    }

    private void processHashFieldPack(KeyEntry entry) throws IOException {
        processHashFieldPack(shard, entry);
    }

    /**
     * Loads data from the shard's volume using the provided transaction. Iterates over all KeyEntry
     * objects retrieved from the shard's volume and processes them based on their data structure type.
     **/
    private void loadFromVolume(Transaction tr) {
        Session session = new Session(tr, prefix);
        Iterable<KeyEntry> iterable = shard.volume().getRange(session);
        iterable.forEach(entry -> {
            try {
                byte kind = entry.entry().get(0);
                switch (kind) {
                    case StringPack.MAGIC:
                        processStringPack(entry);
                        break;
                    case HashFieldPack.MAGIC:
                        processHashFieldPack(entry);
                        break;
                    default:
                        LOGGER.error(
                                "Invalid data structure magic, Versionstamped key = {}",
                                VersionstampUtils.base64Encode(entry.key())
                        );
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    /**
     * Loads data into the shard from its storage volume and sets its operable state to true.
     * This method uses a FoundationDB transaction to retrieve and process the data stored
     * in the shard's volume. It delegates the data-loading logic to the loadFromVolume method.
     * Upon successful completion, the shard is marked as operable.
     */
    public void load() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            loadFromVolume(tr);
        }
        shard.setOperable(true);
    }
}