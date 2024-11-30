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

package com.kronotop.redis.server;

import com.kronotop.commandbuilder.redis.RedisCommandBuilder;
import com.kronotop.redis.storage.BaseStorageTest;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.syncer.VolumeSyncer;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class FlushDBHandlerTest extends BaseStorageTest {
    @Test
    public void test_FLUSHDB() {
        // Insert some keys to the database.
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            for (int number = 0; number < 10; number++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.select(number).encode(buf);
                channel.writeInbound(buf);
                channel.readOutbound();

                buf = Unpooled.buffer();
                cmd.set(makeKey(number), "myvalue").encode(buf);

                channel.writeInbound(buf);
                Object msg = channel.readOutbound();
                assertInstanceOf(SimpleStringRedisMessage.class, msg);
                SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
                assertEquals(Response.OK, actualMessage.content());
            }
        }

        {
            int shards = kronotopInstance.getContext().getConfig().getInt("redis.shards");
            for (int shardId = 0; shardId < shards; shardId++) {
                // VolumeSync task has been run at the background, but it's an async event.
                // Let's run the task eagerly. It's safe.
                RedisShard shard = redisService.getShard(shardId);
                VolumeSyncer volumeSyncer = new VolumeSyncer(context, shard);
                volumeSyncer.run();
            }
        }

        // Wipe out all the keys
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.flushdb().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // All keys are gone.
        {
            for (int number = 0; number < 10; number++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.select(number).encode(buf);
                channel.writeInbound(buf);
                channel.readOutbound();

                buf = Unpooled.buffer();
                cmd.get(makeKey(number)).encode(buf);

                channel.writeInbound(buf);
                Object msg = channel.readOutbound();
                assertInstanceOf(FullBulkStringRedisMessage.class, msg);
                FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) msg;
                assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, actualMessage);
            }
        }
    }
}
