/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.core.handlers;

import com.kronotop.BaseHandlerTest;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class InfoHandlerTest extends BaseHandlerTest {

    private String runInfo(EmbeddedChannel channel) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*1\r\n$4\r\nINFO\r\n".getBytes(StandardCharsets.US_ASCII));

        Object response = runCommand(channel, buf);
        assertInstanceOf(FullBulkStringRedisMessage.class, response);
        return ((FullBulkStringRedisMessage) response).content().toString(StandardCharsets.US_ASCII);
    }

    @Test
    void shouldReportStandaloneMode() {
        // Behavior: INFO reports redis_mode:standalone and cluster_enabled:0 so
        // clients do not switch to slot-based cluster routing
        String info = runInfo(getChannel());

        assertTrue(info.contains("redis_mode:standalone\r\n"));
        assertTrue(info.contains("cluster_enabled:0\r\n"));
        assertFalse(info.contains("redis_mode:cluster"));
    }

    @Test
    void shouldReturnServerAndClusterSections() {
        // Behavior: INFO returns the Server and Cluster section headers
        String info = runInfo(getChannel());

        assertTrue(info.contains("# Server\r\n"));
        assertTrue(info.contains("# Cluster\r\n"));
        assertTrue(info.contains("kronotop_version:"));
        assertTrue(info.contains("os:"));
    }
}
