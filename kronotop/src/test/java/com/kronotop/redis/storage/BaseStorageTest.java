/*
 * Copyright (c) 2023 Kronotop
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

import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.ConfigTestUtil;
import com.kronotop.KronotopTestInstance;
import com.kronotop.Context;
import com.kronotop.redis.RedisService;
import com.typesafe.config.Config;
import io.lettuce.core.cluster.SlotHash;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;

public class BaseStorageTest {
    public final DirectoryLayer directoryLayer = new DirectoryLayer();
    protected KronotopTestInstance kronotopInstance;
    protected RedisService redisService;
    protected Context context;
    protected EmbeddedChannel channel;

    protected Integer getShardId(String key) {
        int slot = SlotHash.getSlot(key);
        return redisService.getHashSlots().get(slot);
    }

    @BeforeEach
    public void setup() throws UnknownHostException, InterruptedException {
        Config config = ConfigTestUtil.load("test.conf");
        kronotopInstance = new KronotopTestInstance(config);
        kronotopInstance.start();
        context = kronotopInstance.getContext();
        redisService = kronotopInstance.getContext().getService(RedisService.NAME);
        channel = kronotopInstance.getChannel();
    }

    @AfterEach
    public void tearDown() {
        if (kronotopInstance == null) {
            return;
        }
        kronotopInstance.shutdown();
    }
}
