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

package com.kronotop.volume.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.commandbuilder.kronotop.VolumeAdminCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.Session;
import com.kronotop.volume.VolumeStatus;
import com.kronotop.volume.replication.BaseNetworkedVolumeIntegrationTest;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

public class VolumeAdminHandlerTest extends BaseNetworkedVolumeIntegrationTest {

    private void injectTestData() throws IOException {
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(10);
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }
    }

    @Test
    public void test_volume_admin_list() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.list().encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        // the name and number of the volumes is an implementation detail of different components
        assertFalse(actualMessage.children().isEmpty());
    }

    @Test
    public void test_volume_describe() throws IOException {
        injectTestData();

        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.describe("redis-shard-1").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        actualMessage.children().forEach((k, v) -> {
            SimpleStringRedisMessage key = (SimpleStringRedisMessage) k;
            switch (key.content()) {
                case "name":
                    assertEquals("redis-shard-1", ((SimpleStringRedisMessage) v).content());
                    break;
                case "status":
                    assertEquals(VolumeStatus.READWRITE.name(), ((SimpleStringRedisMessage) v).content());
                    break;
                case "data_dir":
                    SimpleStringRedisMessage dataDir = (SimpleStringRedisMessage) v;
                    assertFalse(dataDir.content().isEmpty());
                    break;
                case "segment_size":
                    assert v instanceof IntegerRedisMessage;
                    IntegerRedisMessage segmentSize = (IntegerRedisMessage) v;
                    assertTrue(segmentSize.value() > 0);
                    break;
                case "segments":
                    MapRedisMessage segments = (MapRedisMessage) v;
                    assertFalse(segments.children().isEmpty());
                    segments.children().forEach((kk, vv) -> {
                        String name = ((SimpleStringRedisMessage) kk).content();
                        assertFalse(name.isEmpty());

                        MapRedisMessage sub = (MapRedisMessage) vv;
                        sub.children().forEach((k2, v2) -> {
                            String segmentKey = ((SimpleStringRedisMessage) k2).content();
                            switch (segmentKey) {
                                case "size":
                                    IntegerRedisMessage size = (IntegerRedisMessage) v2;
                                    assertTrue(size.value() > 0);
                                    break;
                                case "free_bytes", "used_bytes", "cardinality":
                                    IntegerRedisMessage freeBytes = (IntegerRedisMessage) v2;
                                    assertTrue(freeBytes.value() > 0);
                                    break;
                                case "garbage_ratio":
                                    // TODO: Waits for KR-9
                                    break;
                            }
                        });
                    });
            }
        });
    }

    @Test
    public void test_volume_admin_set_status() {
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.setStatus("redis-shard-1", "READONLY").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.describe("redis-shard-1").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage actualMessage = (MapRedisMessage) msg;
            actualMessage.children().forEach((k, v) -> {
                SimpleStringRedisMessage key = (SimpleStringRedisMessage) k;
                if (key.content().equals("status")) {
                    assertEquals(VolumeStatus.READONLY.name(), ((SimpleStringRedisMessage) v).content());
                }
            });
        }
    }

}