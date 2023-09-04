/*
 * Copyright (c) 2023 Kronotop
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
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.redis.storage.BaseStorageTest;
import com.kronotop.redis.storage.Partition;
import com.kronotop.redis.storage.persistence.DataStructure;
import com.kronotop.redis.storage.persistence.Persistence;
import com.kronotop.redistest.RedisCommandBuilder;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class FlushAllHandlerTest extends BaseStorageTest {
    @Test
    public void testFLUSHALL() {
        setupRedisService();

        String key = "mykey";

        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            for (int i = 0; i < 5; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.select(i).encode(buf);
                channel.writeInbound(buf);
                channel.readOutbound();

                buf = Unpooled.buffer();
                cmd.set(key, "myvalue").encode(buf);

                channel.writeInbound(buf);
                Object msg = channel.readOutbound();
                assertInstanceOf(SimpleStringRedisMessage.class, msg);
                SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
                assertEquals("OK", actualMessage.content());
            }
        }

        {
            // Persistence task has been run at the background, but it's an async event.
            // Let's run the task eagerly. It's safe.
            for (int i = 0; i < 5; i++) {
                cmd.select(i);
                String logicalDatabaseName = Integer.toString(i);
                Partition partition = redisService.getPartition(logicalDatabaseName, getPartitionId(key));
                Persistence persistence = new Persistence(context, logicalDatabaseName, partition);
                persistence.run();
            }
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.flushall().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        {
            for (int i = 0; i < 5; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.select(i).encode(buf);
                channel.writeInbound(buf);
                channel.readOutbound();

                buf = Unpooled.buffer();
                cmd.get("mykey").encode(buf);

                channel.writeInbound(buf);
                Object msg = channel.readOutbound();
                assertInstanceOf(FullBulkStringRedisMessage.class, msg);
                FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) msg;
                assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, actualMessage);
            }
        }

        {
            Database database = context.getFoundationDB();
            database.run(tr -> {
                for (int i = 0; i < 5; i++) {
                    List<String> subspace = DirectoryLayout.Builder.clusterName(context.getClusterName()).internal().redis().persistence().logicalDatabase(Integer.toString(i)).dataStructure(DataStructure.STRING.name().toLowerCase()).asList();
                    DirectoryLayer directoryLayer = new DirectoryLayer();
                    DirectorySubspace directorySubspace = directoryLayer.createOrOpen(tr, subspace).join();
                    byte[] value = tr.get(directorySubspace.pack("mykey")).join();
                    assertNull(value);
                }
                return null;
            });
        }
    }
}
