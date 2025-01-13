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

package com.kronotop.redis.handlers.hash;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.BaseHandler;
import com.kronotop.redis.handlers.hash.protocol.FieldValuePair;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.syncer.jobs.AppendHashFieldJob;
import com.kronotop.redis.storage.syncer.jobs.DeleteByVersionstampJob;

public class BaseHashHandler extends BaseHandler {

    public BaseHashHandler(RedisService service) {
        super(service);
    }

    protected void syncHashField(RedisShard shard, String key, FieldValuePair fieldValuePair) {
        AppendHashFieldJob job = new AppendHashFieldJob(key, fieldValuePair.getField());
        shard.volumeSyncQueue().add(job);
    }

    protected void deleteByVersionstamp(RedisShard shard, HashFieldValue hashField) {
        if (hashField.versionstamp() == null) {
            return;
        }
        DeleteByVersionstampJob job = new DeleteByVersionstampJob(hashField.versionstamp());
        shard.volumeSyncQueue().add(job);
    }
}
