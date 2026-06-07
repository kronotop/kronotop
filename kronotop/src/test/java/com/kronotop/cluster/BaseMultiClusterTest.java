/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.cluster;

import com.kronotop.BaseTest;
import com.kronotop.KronotopTestInstance;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.commands.KrAdminCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class BaseMultiClusterTest extends BaseTest {
    static final String TEST_BUCKET = "test-bucket";
    static final int SHARD_ID = 1;

    KronotopTestInstance clusterA;
    KronotopTestInstance clusterB;

    private Config loadConfigWithClusterName(String clusterName) {
        Path tempDir = Paths.get(temporaryParentDataDir.getAbsolutePath(), UUID.randomUUID().toString());
        Config preConfig = ConfigFactory.load("test.conf");
        Map<String, ConfigValue> map = preConfig.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        map.put("cluster.name", ConfigValueFactory.fromAnyRef(clusterName));
        map.put("data_dir", ConfigValueFactory.fromAnyRef(tempDir.toString()));
        return ConfigFactory.parseMap(map);
    }

    @BeforeEach
    void setup() throws Exception {
        Config configA = loadConfigWithClusterName("cluster-a-" + UUID.randomUUID());
        clusterA = new KronotopTestInstance(configA);
        clusterA.start();

        Config configB = loadConfigWithClusterName("cluster-b-" + UUID.randomUUID());
        clusterB = new KronotopTestInstance(configB);
        clusterB.start();
    }

    @AfterEach
    void tearDown() {
        if (clusterA != null) {
            clusterA.shutdown();
        }
        if (clusterB != null) {
            clusterB.shutdown();
        }
    }

    void createBucket(EmbeddedChannel channel) {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.shards(List.of(SHARD_ID)).ifNotExists()).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
    }

    void insertDocument(EmbeddedChannel channel, String json) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[] document = BSONUtil.jsonToDocumentThenBytes(json);
        cmd.insert(TEST_BUCKET, document).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, response);
        assertEquals(1, ((ArrayRedisMessage) response).children().size());
    }

    List<String> listBuckets(EmbeddedChannel channel) {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.list().encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        return array.children().stream()
                .map(msg -> ((FullBulkStringRedisMessage) msg).content().toString(StandardCharsets.UTF_8))
                .toList();
    }

    void switchToResp3(EmbeddedChannel channel) {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.hello(RESPVersion.RESP3.getValue()).encode(buf);
        runCommand(channel, buf);
    }

    void dropCluster(KronotopTestInstance instance) {
        String clusterName = instance.getContext().getClusterName();
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        // Step 1: request token
        ByteBuf buf = Unpooled.buffer();
        cmd.dropCluster(clusterName).encode(buf);
        Object response = runCommand(instance.getChannel(), buf);
        assertInstanceOf(FullBulkStringRedisMessage.class, response);
        String token = ((FullBulkStringRedisMessage) response).content().toString(StandardCharsets.UTF_8);

        // Step 2: confirm with token
        ByteBuf confirmBuf = Unpooled.buffer();
        cmd.dropCluster(clusterName, token).encode(confirmBuf);
        Object confirmResponse = runCommand(instance.getChannel(), confirmBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, confirmResponse);
    }

    List<BsonDocument> queryAll(EmbeddedChannel channel) {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, response);

        MapRedisMessage map = (MapRedisMessage) response;
        RedisMessage entriesMsg = null;
        for (Map.Entry<RedisMessage, RedisMessage> entry : map.children().entrySet()) {
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) entry.getKey();
            if (key.content().toString(StandardCharsets.UTF_8).equals("entries")) {
                entriesMsg = entry.getValue();
                break;
            }
        }
        assertNotNull(entriesMsg);
        assertInstanceOf(ArrayRedisMessage.class, entriesMsg);

        ArrayRedisMessage entriesArray = (ArrayRedisMessage) entriesMsg;
        return entriesArray.children().stream()
                .map(msg -> {
                    byte[] bytes = ByteBufUtil.getBytes(((FullBulkStringRedisMessage) msg).content());
                    return BSONUtil.toBsonDocument(bytes);
                })
                .toList();
    }
}
