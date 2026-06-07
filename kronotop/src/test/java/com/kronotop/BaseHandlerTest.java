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

package com.kronotop;

import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.RESPVersion;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;
import java.util.UUID;

public class BaseHandlerTest extends BaseStandaloneInstanceTest {
    protected final int SHARD_ID = 1;
    protected final String TEST_BUCKET = "test-bucket";

    protected EmbeddedChannel channel;
    protected String namespace;

    public EmbeddedChannel getChannel() {
        return channel;
    }

    protected EmbeddedChannel newChannel() {
        return instance.newChannel();
    }

    @BeforeEach
    public void setup() throws UnknownHostException, InterruptedException {
        super.setup();
        channel = newChannel();
        namespace = UUID.randomUUID().toString();
    }

    protected void switchProtocol(RESPVersion version) {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.hello(version.getValue()).encode(buf);
        runCommand(channel, buf);
    }
}