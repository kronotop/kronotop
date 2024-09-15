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
import com.kronotop.redis.hash.HashFieldValue;
import com.kronotop.redis.storage.persistence.HashFieldPack;
import com.kronotop.redis.storage.persistence.PersistenceSession;
import com.kronotop.redis.storage.persistence.RedisValueContainer;

import java.util.concurrent.locks.ReadWriteLock;

public class AppendHashFieldJob implements PersistenceJob {
    private final String key;
    private final String field;
    private int position;

    public AppendHashFieldJob(String key, String field) {
        this.key = key;
        this.field = field;
    }

    public String key() {
        return key;
    }

    public String field() {
        return field;
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
            HashFieldValue hashFieldValue = container.hash().get(field);
            if (hashFieldValue == null) {
                // Removed before calling this job
                return;
            }
            HashFieldPack hashFieldPack = new HashFieldPack(key, field, hashFieldValue);
            position = session.pack(hashFieldPack);
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
            container.hash().computeIfPresent(field, (f, hashFieldValue) -> {
                hashFieldValue.setVersionstamp(versionstamp);
                return hashFieldValue;
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private int hashCodeInternal() {
        int result = key.hashCode();
        result = 31 * result + field.hashCode();
        return result;
    }

    @Override
    public int hashCode() {
        return hashCodeInternal();
    }
}
