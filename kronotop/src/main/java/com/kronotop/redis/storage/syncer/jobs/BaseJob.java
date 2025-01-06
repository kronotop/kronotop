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

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.storage.syncer.VolumeSyncSession;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.BiConsumer;

public class BaseJob {
    protected int hashCode;

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof VolumeSyncJob other)) {
            return false;
        }
        return hashCode == other.hashCode();
    }

    protected void linkRedisValueWithVolumeEntry(VolumeSyncSession session, String key, int position, BiConsumer<RedisValueContainer, Versionstamp> syncFunction) {
        Versionstamp[] versionstamps = session.versionstamps();
        if (versionstamps == null || versionstamps.length == 0) {
            return;
        }

        Versionstamp versionstamp = versionstamps[position];
        ReadWriteLock lock = session.shard().striped().get(key);
        lock.writeLock().lock();
        try {
            RedisValueContainer container = session.shard().storage().get(key);
            syncFunction.accept(container, versionstamp);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
