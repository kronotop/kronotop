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

package com.kronotop.redis.storage.syncer.jobs;

import com.kronotop.redis.handlers.hash.HashFieldValue;
import com.kronotop.redis.storage.HashFieldPack;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.storage.syncer.VolumeSyncSession;

import java.util.concurrent.locks.ReadWriteLock;

public class AppendHashFieldJob extends BaseJob implements VolumeSyncJob {
    private final String key;
    private final String field;
    private int position;

    public AppendHashFieldJob(String key, String field) {
        this.key = key;
        this.field = field;
        this.hashCode = hashCodeInternal();
    }

    public String key() {
        return key;
    }

    public String field() {
        return field;
    }

    @Override
    public void run(VolumeSyncSession session) {
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
    public void postHook(VolumeSyncSession session) {
        linkRedisValueWithVolumeEntry(session, key, position, (container, versionstamp) -> {
            if (container == null) {
                // Deleted after persisting it to the shard's volume.
                return;
            }
            container.hash().computeIfPresent(field, (f, hashFieldValue) -> {
                hashFieldValue.setVersionstamp(versionstamp);
                return hashFieldValue;
            });
        });
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private int hashCodeInternal() {
        assert key != null;
        assert field != null;

        int result = key.hashCode();
        result = 31 * result + field.hashCode();
        return result;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
