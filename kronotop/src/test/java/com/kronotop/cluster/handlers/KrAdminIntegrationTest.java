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

package com.kronotop.cluster.handlers;

import com.kronotop.BaseUninitializedKronotopInstanceTest;
import com.kronotop.cluster.MemberIdGenerator;
import com.kronotop.cluster.RouteKind;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class KrAdminIntegrationTest extends BaseUninitializedKronotopInstanceTest {

    private void initializeCluster(KrAdminCommandBuilder<String, String> cmd) {
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.initializeCluster().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }
    }

    @Test
    public void when_cluster_uninitialized() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        // it can be any command that requires an initialized cluster.
        cmd.route("SET", "STANDBY", "REDIS", 1, MemberIdGenerator.generateId()).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR cluster has not been initialized yet", actualMessage.content());
    }

    @Test
    public void try_assign_standby_while_no_primary_assigned_yet() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        initializeCluster(cmd);

        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", "STANDBY", "REDIS", 1, kronotopInstance.getMember().getId()).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR no primary member assigned yet", actualMessage.content());
    }

    @Test
    public void try_set_route_but_member_not_found() {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        initializeCluster(cmd);

        for (RouteKind routeKind : RouteKind.values()) {
            ByteBuf buf = Unpooled.buffer();
            cmd.route("SET", routeKind.name(), "REDIS", 1, MemberIdGenerator.generateId()).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertEquals("ERR member not found", actualMessage.content());
        }
    }
}
