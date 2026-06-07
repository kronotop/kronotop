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

package com.kronotop.server;

import com.kronotop.BaseHandlerTest;
import com.kronotop.MemberAttributes;
import com.kronotop.commands.redis.RedisCommandBuilder;
import com.kronotop.instance.KronotopInstanceStatus;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GracefulShutdownTest extends BaseHandlerTest {

    @Test
    void shouldRejectCommandsWhenInstanceIsStopped() {
        // Behavior: When instance status is STOPPED, new commands are rejected with "server is shutting down".
        context.getMemberAttributes()
                .attr(MemberAttributes.INSTANCE_STATUS)
                .set(KronotopInstanceStatus.STOPPED);

        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.ping().encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage error = (ErrorRedisMessage) response;
        assertTrue(error.content().contains("server is shutting down"));
    }

    @Test
    void shouldAcceptCommandsWhenInstanceIsRunning() {
        // Behavior: When instance status is RUNNING, commands work normally.
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.ping().encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
    }
}
