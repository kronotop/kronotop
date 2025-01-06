/*
 * Copyright (c) 2023-2025 Kronotop
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

import com.kronotop.redis.storage.BaseRedisValue;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.storage.StringPack;
import com.kronotop.redis.storage.syncer.VolumeSyncSession;

import java.util.concurrent.locks.ReadWriteLock;

public class AppendStringJob extends BaseJob implements VolumeSyncJob {
    private final String key;
    private int position;

    public AppendStringJob(String key) {
        this.key = key;
        this.hashCode = hashCodeInternal();
    }

    public String key() {
        return key;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private int hashCodeInternal() {
        return key.hashCode() * 23;
    }

    @Override
    public int hashCode() {
        return hashCode;
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
            StringPack stringPack = new StringPack(key, container.string());
            position = session.pack(stringPack);
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
            BaseRedisValue<?> value = container.baseRedisValue();
            value.setVersionstamp(versionstamp);
        });
    }
}
