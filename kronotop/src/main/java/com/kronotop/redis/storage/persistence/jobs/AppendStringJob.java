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

package com.kronotop.redis.storage.persistence.jobs;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.redis.storage.persistence.BaseRedisValue;
import com.kronotop.redis.storage.persistence.PersistenceSession;
import com.kronotop.redis.storage.persistence.RedisValueContainer;
import com.kronotop.redis.storage.persistence.StringPack;

import java.util.concurrent.locks.ReadWriteLock;

public class AppendStringJob implements PersistenceJob {
    private final String key;
    private int position;

    public AppendStringJob(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }

    @Override
    public void run(PersistenceSession session) {
        ReadWriteLock lock = session.shard().striped().get(key);
        lock.readLock().lock();
        try {
            RedisValueContainer container = session.shard().storage().get(key);
            if (container == null) {
                // Removed before calling this job
                return;
            }
            StringPack stringPack = new StringPack(key, container.string());
            position = session.pack(stringPack);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void postHook(PersistenceSession session, Versionstamp[] versionstampedKeys) {
        Versionstamp versionstamp = versionstampedKeys[position];
        ReadWriteLock lock = session.shard().striped().get(key);
        lock.writeLock().lock();
        try {
            RedisValueContainer container = session.shard().storage().get(key);
            if (container == null) {
                // Deleted after persisting it to the shard's volume.
                return;
            }
            BaseRedisValue<?> value = container.baseRedisValue();
            value.setVersionstamp(versionstamp);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
