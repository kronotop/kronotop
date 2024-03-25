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

package com.kronotop.sql.executor;

import com.kronotop.ConfigTestUtil;
import com.kronotop.KronotopTestInstance;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.protocol.SqlArgs;
import com.kronotop.server.MockChannelHandlerContext;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.impl.RespRequest;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.KronotopSchema;
import com.kronotop.sql.KronotopTable;
import com.kronotop.sql.AssertResponse;
import com.kronotop.sql.metadata.SchemaMetadata;
import com.kronotop.sql.metadata.SchemaNotExistsException;
import com.kronotop.sql.metadata.SqlMetadataService;
import com.kronotop.sql.metadata.TableNotExistsException;
import com.kronotop.sql.optimizer.Optimize;
import com.kronotop.sql.optimizer.QueryOptimizationResult;
import com.kronotop.sql.protocol.SqlMessage;
import com.typesafe.config.Config;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class BasePlanIntegrationTest {
    protected final String DEFAULT_SCHEMA = "public";
    protected KronotopTestInstance kronotopInstance;
    protected ChannelHandlerContext channelContext;
    protected EmbeddedChannel channel;

    protected void setupCommon(Config config) throws UnknownHostException, InterruptedException {
        kronotopInstance = new KronotopTestInstance(config);
        kronotopInstance.start();
        channel = kronotopInstance.getChannel();
        channelContext = new MockChannelHandlerContext(channel);
    }

    @BeforeEach
    public void setup() throws UnknownHostException, InterruptedException {
        Config config = ConfigTestUtil.load("test.conf");
        setupCommon(config);
    }

    @AfterEach
    public void tearDown() {
        if (kronotopInstance == null) {
            return;
        }
        kronotopInstance.shutdown();
    }

    protected void executeSQLQuery(String query) {
        executeSQLQuery(query, 1);
    }

    protected void executeSQLQuery(String query, int responseSize) {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sql(SqlArgs.Builder.query(query)).encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
        SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, responseSize);
        assertEquals(Response.OK, message.content());
    }

    protected PlanContext execute(String query) {
        PlanExecutor executor = new PlanExecutor(kronotopInstance.getContext());
        ChannelHandlerContext context = new MockChannelHandlerContext(kronotopInstance.newChannel());

        List<RedisMessage> messages = new ArrayList<>();
        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes("SQL".getBytes())));
        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes(query.getBytes())));

        Request request = new RespRequest(context, new ArrayRedisMessage(messages));
        PlanContext planContext = new PlanContext(channelContext, new SqlMessage(request));
        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        try {
            SchemaMetadata schemaMetadata = sqlMetadataService.findSchemaMetadata("public");
            KronotopSchema kronotopSchema = schemaMetadata.getKronotopSchema();
            QueryOptimizationResult result = Optimize.optimize(kronotopSchema, query);
            Plan plan = new Plan(result);
            executor.execute(planContext, plan);
            return planContext;
        } catch (SchemaNotExistsException | SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    protected void awaitSchemaMetadataForTable(String schema, String table) {
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                SqlMetadataService metadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
                KronotopTable kronotopTable = metadataService.findTable(schema, table);
                assertNotNull(kronotopTable);
            } catch (SchemaNotExistsException | TableNotExistsException e) {
                return false;
            }
            return true;
        });
    }
}
