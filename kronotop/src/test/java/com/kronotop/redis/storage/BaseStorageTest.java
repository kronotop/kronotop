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

import com.kronotop.BaseTest;
import com.kronotop.Context;
import com.kronotop.KronotopTestInstance;
import com.kronotop.redis.RedisService;
import com.kronotop.volume.Prefix;
import com.typesafe.config.Config;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;

public class BaseStorageTest extends BaseTest {
    protected KronotopTestInstance kronotopInstance;
    protected RedisService redisService;
    protected Context context;
    protected EmbeddedChannel channel;
    protected Prefix redisVolumeSyncerPrefix;


    @BeforeEach
    public void setup() throws UnknownHostException, InterruptedException {
        Config config = loadConfig("test.conf");
        kronotopInstance = new KronotopTestInstance(config);
        kronotopInstance.start();
        context = kronotopInstance.getContext();
        redisService = kronotopInstance.getContext().getService(RedisService.NAME);
        channel = kronotopInstance.getChannel();
        redisVolumeSyncerPrefix = new Prefix(context.getConfig().getString("redis.volume_syncer.prefix").getBytes());
    }

    @AfterEach
    public void tearDown() {
        if (kronotopInstance == null) {
            return;
        }
        kronotopInstance.shutdown();
    }
}
