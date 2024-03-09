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

package com.kronotop.sql.plan;

import com.kronotop.ConfigTestUtil;
import com.kronotop.KronotopTestInstance;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.MockChannelHandlerContext;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.KronotopSchema;
import com.kronotop.sql.KronotopTable;
import com.kronotop.sql.Plan;
import com.kronotop.sql.backend.AssertResponse;
import com.kronotop.sql.backend.metadata.SchemaMetadata;
import com.kronotop.sql.backend.metadata.SchemaNotExistsException;
import com.kronotop.sql.backend.metadata.SqlMetadataService;
import com.kronotop.sql.backend.metadata.TableNotExistsException;
import com.kronotop.sql.optimizer.Optimize;
import com.typesafe.config.Config;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class PlanExecutorIntegrationTest {
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

    private void executeSQLQuery(String query) {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sql(query).encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
        SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
        assertEquals(Response.OK, message.content());
    }

    private PlanContext execute(String query) {
        PlanExecutor executor = new PlanExecutor(kronotopInstance.getContext());
        PlanContext planContext = new PlanContext(channelContext);
        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        try {
            SchemaMetadata schemaMetadata = sqlMetadataService.findSchemaMetadata("public");
            KronotopSchema kronotopSchema = schemaMetadata.getKronotopSchema();
            RelNode relNode = Optimize.optimize(kronotopSchema, query);
            Plan plan = new Plan(relNode);
            executor.execute(planContext, plan);
            return planContext;
        } catch (SchemaNotExistsException | SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    private void awaitSchemaMetadataForTable(String schema, String table) {
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

    @Nested
    class TestPlanContext_SupportedDataTypes {
        private void createTable(String query) {
            executeSQLQuery(query);
            awaitSchemaMetadataForTable("public", "supported_datatype");
        }

        @AfterEach
        public void afterEach() {
            executeSQLQuery("DROP TABLE supported_datatype");
        }

        @Test
        void when_scalar_type_BOOLEAN() {
            // Logical values Values: TRUE, FALSE, UNKNOWN
            createTable("CREATE TABLE supported_datatype (boolean_column BOOLEAN)");
            PlanContext planContext = execute("INSERT INTO supported_datatype (boolean_column) values(TRUE)");
            Boolean value = planContext.getRows().get(0).get("boolean_column").getValueAs(Boolean.class);
            assertNotNull(value);
            assertEquals(true, value);
        }

        @Test
        void when_scalar_type_TINYINT() {
            // 1 byte signed integer. Range is -128 to 127.
            createTable("CREATE TABLE supported_datatype (tinyint_column TINYINT)");
            PlanContext planContext = execute("INSERT INTO supported_datatype (tinyint_column) values(35)");
            Byte value = planContext.getRows().get(0).get("tinyint_column").getValueAs(Byte.class);
            assertNotNull(value);
            assertEquals((byte) 35, value);
        }

        @Test
        void when_scalar_type_SMALLINT() {
            // 2 byte signed integer. Range is -32768 to 32767.
            createTable("CREATE TABLE supported_datatype (smallint_column SMALLINT)");
            PlanContext planContext = execute("INSERT INTO supported_datatype (smallint_column) values(35)");
            Short value = planContext.getRows().get(0).get("smallint_column").getValueAs(Short.class);
            assertNotNull(value);
            assertEquals((short) 35, value);
        }

        @Test
        void when_scalar_type_INTEGER() {
            // 4 byte signed integer. Range is -2147483648 to 2147483647.
            createTable("CREATE TABLE supported_datatype (integer_column INTEGER)");
            PlanContext planContext = execute("INSERT INTO supported_datatype (integer_column) values(35)");
            Integer value = planContext.getRows().get(0).get("integer_column").getValueAs(Integer.class);
            assertNotNull(value);
            assertEquals(35, value);
        }

        @Test
        void when_scalar_type_INT() {
            // 4 byte signed integer. Range is -2147483648 to 2147483647.
            createTable("CREATE TABLE supported_datatype (int_column INT)");
            PlanContext planContext = execute("INSERT INTO supported_datatype (int_column) values(35)");
            Integer value = planContext.getRows().get(0).get("int_column").getValueAs(Integer.class);
            assertNotNull(value);
            assertEquals(35, value);
        }

        @Test
        void when_scalar_type_BIGINT() {
            // 8 byte signed integer. Range is -9223372036854775808 to 9223372036854775807.
            createTable("CREATE TABLE supported_datatype (bigint_column BIGINT)");
            PlanContext planContext = execute("INSERT INTO supported_datatype (bigint_column) values(35)");
            Double value = planContext.getRows().get(0).get("bigint_column").getValueAs(Double.class);
            assertNotNull(value);
            assertEquals(35, value);
        }

        @Test
        void when_scalar_type_DECIMAL() {
            // Fixed point	Example: 123.45 and DECIMAL ‘123.45’ are identical values, and have type DECIMAL(5, 2)
            createTable("CREATE TABLE supported_datatype (decimal_column DECIMAL)");
            PlanContext planContext = execute("INSERT INTO supported_datatype (decimal_column) values(123.45)");
            Float value = planContext.getRows().get(0).get("decimal_column").getValueAs(Float.class);
            assertNotNull(value);
            assertEquals((float) 123.45, value);
        }

        @Test
        void when_scalar_type_NUMERIC() {
            // A synonym for DECIMAL
            createTable("CREATE TABLE supported_datatype (numeric_column NUMERIC)");
            PlanContext planContext = execute("INSERT INTO supported_datatype (numeric_column) values(123.45)");
            Float value = planContext.getRows().get(0).get("numeric_column").getValueAs(Float.class);
            assertNotNull(value);
            assertEquals((float) 123.45, value);
        }

        @Test
        void when_scalar_type_REAL() {
            // 4 byte floating point. 6 decimal digits precision; examples: CAST(1.2 AS REAL), CAST(‘Infinity’ AS REAL)
            createTable("CREATE TABLE supported_datatype (real_column REAL)");
            PlanContext planContext = execute("INSERT INTO supported_datatype (real_column) VALUES(CAST(1.2 AS REAL))");
            Float value = planContext.getRows().get(0).get("real_column").getValueAs(Float.class);
            assertNotNull(value);
            assertEquals((float) 1.2, value);
        }

        @Test
        void when_scalar_type_DOUBLE() {
            // 8 byte floating point. 15 decimal digits precision; examples: 1.4E2, CAST(‘-Infinity’ AS DOUBLE), CAST(‘NaN’ AS DOUBLE)
            createTable("CREATE TABLE supported_datatype (double_column DOUBLE)");
            PlanContext planContext = execute("INSERT INTO supported_datatype (double_column) VALUES(1.4E2)");
            Double value = planContext.getRows().get(0).get("double_column").getValueAs(Double.class);
            assertNotNull(value);
            assertEquals(1.4E2, value);
        }

        @Test
        void when_scalar_type_FLOAT() {
            // 8 byte floating point. A synonym for DOUBLE.
            createTable("CREATE TABLE supported_datatype (float_column FLOAT)");
            PlanContext planContext = execute("INSERT INTO supported_datatype (float_column) VALUES(1.4E2)");
            Double value = planContext.getRows().get(0).get("float_column").getValueAs(Double.class);
            assertNotNull(value);
            assertEquals(1.4E2, value);
        }

        @Test
        void when_scalar_type_CHAR() {
            // CHAR(n), CHARACTER(n). Fixed-width character string ‘Hello’, ‘’ (empty string), _latin1’Hello’, n’Hello’,
            // _UTF16’Hello’, ‘Hello’ ‘there’ (literal split into multiple parts), e’Hello\nthere’ (literal containing C-style escapes)
            createTable("CREATE TABLE supported_datatype (char_column CHAR(6))");
            PlanContext planContext = execute("INSERT INTO supported_datatype (char_column) values('foobar')");
            String value = planContext.getRows().get(0).get("char_column").getValueAs(String.class);
            assertNotNull(value);
            assertEquals("foobar", value);
        }

        @Test
        void when_scalar_type_CHARACTER() {
            // CHAR(n), CHARACTER(n). Fixed-width character string ‘Hello’, ‘’ (empty string), _latin1’Hello’, n’Hello’,
            // _UTF16’Hello’, ‘Hello’ ‘there’ (literal split into multiple parts), e’Hello\nthere’ (literal containing C-style escapes)
            createTable("CREATE TABLE supported_datatype (character_column CHARACTER(6))");
            PlanContext planContext = execute("INSERT INTO supported_datatype (character_column) values('foobar')");
            String value = planContext.getRows().get(0).get("character_column").getValueAs(String.class);
            assertNotNull(value);
            assertEquals("foobar", value);
        }

        @Test
        void when_scalar_type_VARCHAR() {
            // Variable-length character string. As CHAR(n).
            createTable("CREATE TABLE supported_datatype (varchar_column CHAR(6))");
            PlanContext planContext = execute("INSERT INTO supported_datatype (varchar_column) values('foobar')");
            String value = planContext.getRows().get(0).get("varchar_column").getValueAs(String.class);
            assertNotNull(value);
            assertEquals("foobar", value);
        }

        @Test
        void when_scalar_type_CHARACTER_VARYING() {
            // Variable-length character string. As CHAR(n).
            createTable("CREATE TABLE supported_datatype (varying_column CHARACTER VARYING(6))");
            PlanContext planContext = execute("INSERT INTO supported_datatype (varying_column) values('foobar')");
            String value = planContext.getRows().get(0).get("varying_column").getValueAs(String.class);
            assertNotNull(value);
            assertEquals("foobar", value);
        }

        @Test
        void when_scalar_type_DATE() {
            createTable("CREATE TABLE supported_datatype (date_column DATE)");
            PlanContext planContext = execute("INSERT INTO supported_datatype (date_column) values('1988-07-18')");
            Integer value = planContext.getRows().get(0).get("date_column").getValueAs(Integer.class);
            assertNotNull(value);
            // days since epoch
            assertEquals(6773, value);
        }

        @Test
        void when_scalar_type_TIME() {
            createTable("CREATE TABLE supported_datatype (time_column TIME)");
            PlanContext planContext = execute("INSERT INTO supported_datatype (time_column) values('20:17:40')");
            Integer value = planContext.getRows().get(0).get("time_column").getValueAs(Integer.class);
            assertNotNull(value);
            // millis of day
            assertEquals(73060000, value);
        }

        @Test
        void when_scalar_type_TIMESTAMP() {
            createTable("CREATE TABLE supported_datatype (timestamp_column TIMESTAMP)");
            PlanContext planContext = execute("INSERT INTO supported_datatype (timestamp_column) values('1969-07-20 20:17:40')");
            Long value = planContext.getRows().get(0).get("timestamp_column").getValueAs(Long.class);
            assertNotNull(value);
            // Milliseconds since 1970-01-01 00:00:00
            assertEquals(Long.valueOf("-14182940000"), value);
        }
    }

    @Nested
    class TestWhenIntegerColumnHasDefaultValue {
        @BeforeEach
        public void beforeEach() {
            executeSQLQuery("CREATE TABLE integer_test (integer_column INTEGER DEFAULT 18, string_column VARCHAR)");
            awaitSchemaMetadataForTable("public", "integer_test");
        }

        @AfterEach
        public void afterEach() {
            executeSQLQuery("DROP TABLE integer_test");
        }

        @Test
        public void when_DEFAULT_in_query() {
            PlanContext planContext = execute("INSERT INTO integer_test (integer_column, string_column) values(DEFAULT, 'some string')");
            assertEquals(planContext.getRows().get(0).get("integer_column").getValueAs(Integer.class), 18);
            assertEquals(planContext.getRows().get(0).get("string_column").getValueAs(String.class), "some string");
        }

        @Test
        public void when_no_integer_column_in_query() {
            PlanContext planContext = execute("INSERT INTO integer_test (string_column) values('some string')");
            assertEquals(planContext.getRows().get(0).get("integer_column").getValueAs(Integer.class), 18);
            assertEquals(planContext.getRows().get(0).get("string_column").getValueAs(String.class), "some string");
        }

        @Test
        public void when_integer_column_in_query() {
            PlanContext planContext = execute("INSERT INTO integer_test (integer_column, string_column) values(35, 'some string')");
            assertEquals(planContext.getRows().get(0).get("integer_column").getValueAs(Integer.class), 35);
            assertEquals(planContext.getRows().get(0).get("string_column").getValueAs(String.class), "some string");
        }
    }

    @Nested
    class TestStoredColumns {
        @Test
        public void when_id_column_in_CREATE_TABLE() {
            KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

            ByteBuf buf = Unpooled.buffer();
            cmd.sql("CREATE TABLE virtual_column_test(id VARCHAR)").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<ErrorRedisMessage> assertResponse = new AssertResponse<>();
            ErrorRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals("SQL Cannot CREATE generated column 'id'", message.content());
        }

        @Test
        public void when_id_column_in_ALTER_TABLE_ADD_COLUMN() {
            KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

            ByteBuf buf = Unpooled.buffer();
            cmd.sql("ALTER TABLE public.users ADD COLUMN id").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<ErrorRedisMessage> assertResponse = new AssertResponse<>();
            ErrorRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals("SQL Cannot ALTER generated column 'id'", message.content());
        }

        @Test
        public void when_id_column_in_ALTER_TABLE_DROP_COLUMN() {
            KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

            ByteBuf buf = Unpooled.buffer();
            cmd.sql("ALTER TABLE public.users DROP COLUMN id").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<ErrorRedisMessage> assertResponse = new AssertResponse<>();
            ErrorRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals("SQL Cannot ALTER generated column 'id'", message.content());
        }

        @Test
        public void when_id_column_in_ALTER_TABLE_RENAME_COLUMN() {
            KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

            ByteBuf buf = Unpooled.buffer();
            cmd.sql("ALTER TABLE public.users RENAME COLUMN id TO foobar").encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            AssertResponse<ErrorRedisMessage> assertResponse = new AssertResponse<>();
            ErrorRedisMessage message = assertResponse.getMessage(response, 0, 1);
            assertEquals("SQL Cannot ALTER generated column 'id'", message.content());
        }

        @Test
        public void when_id_column_in_INSERT_INTO() {
            KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

            {
                ByteBuf buf = Unpooled.buffer();
                cmd.sql("CREATE TABLE virtual_column_test(string_column VARCHAR)").encode(buf);
                channel.writeInbound(buf);
                Object response = channel.readOutbound();

                AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
                SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
                assertEquals(Response.OK, message.content());
            }

            {

                awaitSchemaMetadataForTable("public", "virtual_column_test");
                ByteBuf buf = Unpooled.buffer();
                cmd.sql("INSERT INTO virtual_column_test(id, string_column) VALUES('some value', 'other value')").encode(buf);
                channel.writeInbound(buf);
                Object response = channel.readOutbound();

                AssertResponse<ErrorRedisMessage> assertResponse = new AssertResponse<>();
                ErrorRedisMessage message = assertResponse.getMessage(response, 0, 1);
                assertEquals("SQL Cannot INSERT into generated column 'id'", message.content());
            }
        }
    }
}
