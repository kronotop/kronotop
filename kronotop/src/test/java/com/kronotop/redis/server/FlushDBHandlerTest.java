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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.commandbuilder.redis.RedisCommandBuilder;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.storage.BaseStorageTest;
import com.kronotop.redis.storage.DataStructure;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.syncer.VolumeSyncer;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class FlushDBHandlerTest extends BaseStorageTest {
    @Test
    public void test_FLUSHDB() {
        String key = "mykey";

        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set(key, "myvalue").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        {
            // VolumeSync task has been run at the background, but it's an async event.
            // Let's run the task eagerly. It's safe.
            RedisShard shard = redisService.getShard(getShardId(key));
            VolumeSyncer volumeSyncer = new VolumeSyncer(context, shard);
            volumeSyncer.run();
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.flushdb().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.get("mykey").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(FullBulkStringRedisMessage.class, msg);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) msg;
            assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, actualMessage);
        }

        RedisService service = context.getService(RedisService.NAME);
        RedisShard shard = service.findShard("mykey");
        {
            Database database = context.getFoundationDB();
            database.run(tr -> {
                DirectorySubspace subspace = context.getDirectoryLayer().createOrOpenDataStructure(shard.id(), DataStructure.STRING);
                byte[] value = tr.get(subspace.pack("mykey")).join();
                assertNull(value);
                return null;
            });
        }
    }
}
