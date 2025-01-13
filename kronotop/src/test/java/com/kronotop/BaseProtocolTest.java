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

package com.kronotop;

import com.kronotop.server.resp3.RedisArrayAggregator;
import com.kronotop.server.resp3.RedisBulkStringAggregator;
import com.kronotop.server.resp3.RedisDecoder;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class BaseProtocolTest {
    public EmbeddedChannel channel;

    private static EmbeddedChannel newChannel(boolean decodeInlineCommands) {
        return new EmbeddedChannel(
                new RedisDecoder(decodeInlineCommands),
                new RedisBulkStringAggregator(),
                new RedisArrayAggregator());
    }

    @BeforeEach
    public void setup() {
        channel = newChannel(false);
    }

    @AfterEach
    public void teardown() {
        assertFalse(channel.finish());
    }
}