/*
 * Copyright (c) 2023-2025 Kronotop
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

package com.kronotop.redis.handlers.generic;

import com.kronotop.commandbuilder.redis.RedisCommandBuilder;
import com.kronotop.redis.BaseVolumeSyncIntegrationTest;
import com.kronotop.redis.storage.StringPack;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class VolumeSyncIntegrationTest extends BaseVolumeSyncIntegrationTest {

    @Test
    public void test_DEL() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set(key, value).encode(buf);
            channel.writeInbound(buf);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.del(key).encode(buf);
            channel.writeInbound(buf);
        }
        await().atMost(5, TimeUnit.SECONDS).until(() -> !volumeContainsStringKey(key));
    }

    @Test
    public void test_RENAME() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("{0}key", value).encode(buf);
            channel.writeInbound(buf);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.rename("{0}key", "{0}newkey").encode(buf);
            channel.writeInbound(buf);
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume("{0}newkey", keyEntry -> {
            try {
                StringPack pack = StringPack.unpack(keyEntry.entry());
                if (pack.key().equals("{0}newkey")) {
                    String syncedValue = new String(pack.stringValue().value());
                    return syncedValue.equals(value);
                }
                return false;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
    }

    @Test
    public void test_RENAMENX() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set("{0}key", value).encode(buf);
            channel.writeInbound(buf);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.renamenx("{0}key", "{0}newkey").encode(buf);
            channel.writeInbound(buf);
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume("{0}newkey", keyEntry -> {
            try {
                StringPack pack = StringPack.unpack(keyEntry.entry());
                if (pack.key().equals("{0}newkey")) {
                    String syncedValue = new String(pack.stringValue().value());
                    return syncedValue.equals(value);
                }
                return false;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
    }
}
