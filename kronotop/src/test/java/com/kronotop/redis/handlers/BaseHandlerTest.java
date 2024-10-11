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

package com.kronotop.redis.handlers;

import com.kronotop.BaseTest;
import com.kronotop.KronotopTestInstance;
import com.kronotop.volume.Prefix;
import com.typesafe.config.Config;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;

public class BaseHandlerTest extends BaseTest {
    protected KronotopTestInstance kronotopInstance;
    protected EmbeddedChannel channel;
    protected Prefix redisVolumeSyncerPrefix;

    protected EmbeddedChannel newChannel() {
        return kronotopInstance.newChannel();
    }

    protected void setupCommon(Config config) throws UnknownHostException, InterruptedException {
        redisVolumeSyncerPrefix = new Prefix(config.getString("redis.volume_syncer.prefix").getBytes());
        kronotopInstance = new KronotopTestInstance(config);
        kronotopInstance.start();
        channel = kronotopInstance.getChannel();
    }

    @BeforeEach
    public void setup() throws UnknownHostException, InterruptedException {
        Config config = loadConfig("test.conf");
        setupCommon(config);
    }

    @AfterEach
    public void tearDown() {
        if (kronotopInstance == null) {
            return;
        }
        kronotopInstance.shutdown();
    }
}
