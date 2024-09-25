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

package com.kronotop.redis.string;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.redis.BaseHandler;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.storage.syncer.jobs.AppendStringJob;
import com.kronotop.redis.storage.syncer.jobs.DeleteByVersionstampJob;

public class BaseStringHandler extends BaseHandler {
    public BaseStringHandler(RedisService service) {
        super(service);
    }

    protected void deleteByVersionstamp(RedisShard shard, Versionstamp versionstamp) {
        if (!service.isVolumeSyncEnabled()) {
            return;
        }

        if (versionstamp != null) {
            shard.volumeSyncQueue().add(new DeleteByVersionstampJob(versionstamp));
        }
    }

    protected void syncMutatedStringOnVolume(RedisShard shard, String key, Versionstamp versionstamp) {
        if (!service.isVolumeSyncEnabled()) {
            return;
        }

        shard.volumeSyncQueue().add(new AppendStringJob(key));
        if (versionstamp != null) {
            shard.volumeSyncQueue().add(new DeleteByVersionstampJob(versionstamp));
        }
    }

    protected void syncStringOnVolume(RedisShard shard, String key, RedisValueContainer previous) {
        if (!service.isVolumeSyncEnabled()) {
            return;
        }

        if (previous == null) {
            shard.volumeSyncQueue().add(new AppendStringJob(key));
            shard.index().add(key);
        } else {
            shard.volumeSyncQueue().add(new AppendStringJob(key));
            Versionstamp versionstamp = previous.baseRedisValue().versionstamp();
            if (versionstamp != null) {
                shard.volumeSyncQueue().add(new DeleteByVersionstampJob(versionstamp));
            }
        }
    }
}
