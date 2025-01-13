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

package com.kronotop.task.handlers;

import com.kronotop.BaseClusterTest;
import com.kronotop.KronotopTestInstance;
import com.kronotop.commandbuilder.kronotop.TaskAdminCommandBuilder;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class TaskAdminHandlerTest extends BaseClusterTest {
    @Test
    public void test_list() {
        TaskAdminCommandBuilder<String, String> cmd = new TaskAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.list().encode(buf);

        KronotopTestInstance instance = getInstances().getFirst();
        instance.getChannel().writeInbound(buf);
        Object msg = instance.getChannel().readOutbound();
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        actualMessage.children().forEach((name, properties) -> {
            MapRedisMessage task = (MapRedisMessage) properties;
            assertEquals(4, task.children().size());
            Set<String> keys = new HashSet<>();
            task.children().forEach((key, value) -> {
                keys.add(((SimpleStringRedisMessage) key).content());
            });

            // Expected keys
            for (String key : List.of("running", "started_at", "last_run")) {
                assertTrue(keys.contains(key));
            }
        });
    }
}
