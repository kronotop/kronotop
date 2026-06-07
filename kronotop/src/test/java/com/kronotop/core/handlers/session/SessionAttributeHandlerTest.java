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

package com.kronotop.core.handlers.session;

import com.kronotop.BaseHandlerTest;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.SessionAttributeKeywords;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SessionAttributeHandlerTest extends BaseHandlerTest {

    @Test
    void shouldInitializeDefaultValues() {
        // Verify default values from test.conf and reference.conf
        // test.conf overrides: input_type=bson, reply_type=bson, object_id_format=bytes
        // reference.conf defaults: limit=100
        verifyAttributeValue("input_type", "bson");
        verifyAttributeValue("reply_type", "bson");
        verifyAttributeValueInteger("limit", 100);
        verifyAttributeValue("object_id_format", "bytes");
    }

    @Test
    void shouldListAllSessionAttributes() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sessionAttributeList().encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, response);

        ArrayRedisMessage arrayMessage = (ArrayRedisMessage) response;
        List<RedisMessage> children = arrayMessage.children();

        // Should have 4 attributes * 2 (key + value) = 8 elements
        assertEquals(8, children.size());

        // Verify attribute names are present
        assertTrue(containsAttribute(children, "reply_type"));
        assertTrue(containsAttribute(children, "input_type"));
        assertTrue(containsAttribute(children, "limit"));
        assertTrue(containsAttribute(children, "object_id_format"));
    }

    @Test
    void shouldSetReplyTypeToJson() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sessionAttributeSet(SessionAttributeKeywords.REPLY_TYPE, "json").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        // Verify the change
        verifyAttributeValue("reply_type", "json");
    }

    @Test
    void shouldSetReplyTypeToBson() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sessionAttributeSet(SessionAttributeKeywords.REPLY_TYPE, "bson").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        verifyAttributeValue("reply_type", "bson");
    }

    @Test
    void shouldSetInputTypeToJson() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sessionAttributeSet(SessionAttributeKeywords.INPUT_TYPE, "json").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        verifyAttributeValue("input_type", "json");
    }

    @Test
    void shouldSetInputTypeToBson() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sessionAttributeSet(SessionAttributeKeywords.INPUT_TYPE, "bson").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        verifyAttributeValue("input_type", "bson");
    }

    @Test
    void shouldSetLimit() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sessionAttributeSet(SessionAttributeKeywords.LIMIT, "50").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        verifyAttributeValueInteger("limit", 50);
    }

    @Test
    void shouldRejectInvalidLimit() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sessionAttributeSet(SessionAttributeKeywords.LIMIT, "0").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        assertTrue(((ErrorRedisMessage) response).content().contains("'limit' must be greater than 0"));
    }

    @Test
    void shouldSetObjectIdFormatToHex() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sessionAttributeSet(SessionAttributeKeywords.OBJECT_ID_FORMAT, "hex").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        verifyAttributeValue("object_id_format", "hex");
    }

    @Test
    void shouldSetVersionstampFormatToBytes() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sessionAttributeSet(SessionAttributeKeywords.OBJECT_ID_FORMAT, "bytes").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        verifyAttributeValue("object_id_format", "bytes");
    }

    @Test
    void shouldRejectInvalidVersionstampFormat() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sessionAttributeSet(SessionAttributeKeywords.OBJECT_ID_FORMAT, "invalid").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        assertTrue(((ErrorRedisMessage) response).content().contains("Invalid versionstamp format"));
    }

    @Test
    void shouldRejectInvalidReplyType() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sessionAttributeSet(SessionAttributeKeywords.REPLY_TYPE, "invalid").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        assertTrue(((ErrorRedisMessage) response).content().contains("Invalid reply type"));
    }

    @Test
    void shouldRejectInvalidInputType() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sessionAttributeSet(SessionAttributeKeywords.INPUT_TYPE, "invalid").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        assertTrue(((ErrorRedisMessage) response).content().contains("Invalid input type"));
    }

    private boolean containsAttribute(List<RedisMessage> children, String attributeName) {
        for (RedisMessage msg : children) {
            if (msg instanceof FullBulkStringRedisMessage fbsm) {
                if (fbsm.content().toString(StandardCharsets.UTF_8).equals(attributeName)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void verifyAttributeValue(String attributeName, String expectedValue) {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sessionAttributeList().encode(buf);

        Object response = runCommand(channel, buf);
        ArrayRedisMessage arrayMessage = (ArrayRedisMessage) response;
        assertNotNull(arrayMessage);
        List<RedisMessage> children = arrayMessage.children();

        for (int i = 0; i < children.size() - 1; i += 2) {
            RedisMessage keyMsg = children.get(i);
            RedisMessage valueMsg = children.get(i + 1);
            if (keyMsg instanceof FullBulkStringRedisMessage fbsm && fbsm.content().toString(StandardCharsets.UTF_8).equals(attributeName)) {
                assertInstanceOf(FullBulkStringRedisMessage.class, valueMsg);
                assertEquals(expectedValue, ((FullBulkStringRedisMessage) valueMsg).content().toString(StandardCharsets.UTF_8));
                return;
            }
        }
        fail("Attribute not found: " + attributeName);
    }

    private void verifyAttributeValueInteger(String attributeName, long expectedValue) {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sessionAttributeList().encode(buf);

        Object response = runCommand(channel, buf);
        ArrayRedisMessage arrayMessage = (ArrayRedisMessage) response;
        assertNotNull(arrayMessage);
        List<RedisMessage> children = arrayMessage.children();

        for (int i = 0; i < children.size() - 1; i += 2) {
            RedisMessage keyMsg = children.get(i);
            RedisMessage valueMsg = children.get(i + 1);
            if (keyMsg instanceof FullBulkStringRedisMessage fbsm && fbsm.content().toString(StandardCharsets.UTF_8).equals(attributeName)) {
                assertInstanceOf(IntegerRedisMessage.class, valueMsg);
                assertEquals(expectedValue, ((IntegerRedisMessage) valueMsg).value());
                return;
            }
        }
        fail("Attribute not found: " + attributeName);
    }

}
