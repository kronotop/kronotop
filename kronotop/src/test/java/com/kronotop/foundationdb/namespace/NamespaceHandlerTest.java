/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.foundationdb.namespace;

import com.apple.foundationdb.Transaction;
import com.kronotop.BaseHandlerTest;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.MockChannelHandlerContext;
import com.kronotop.server.Response;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.PrefixUtil;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class NamespaceHandlerTest extends BaseHandlerTest {

    @Test
    void test_CREATE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceCreate(namespace).encode(buf);

        EmbeddedChannel channel = getChannel();
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void internalNamespaceShouldNotBeAccessible() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        String namespace = "name." + Namespace.INTERNAL_LEAF;

        Map<String, ByteBuf> commands = new HashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);
            commands.put("NAMESPACE CREATE", buf);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceExists(namespace).encode(buf);
            commands.put("NAMESPACE EXISTS", buf);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceList(namespace).encode(buf);
            commands.put("NAMESPACE LIST", buf);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceMove(namespace, "new-namespace").encode(buf);
            commands.put("NAMESPACE LIST - old namespace has __internal__", buf);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceMove("name", namespace).encode(buf);
            commands.put("NAMESPACE LIST - new namespace has __internal__", buf);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(namespace).encode(buf);
            commands.put("NAMESPACE REMOVE", buf);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(namespace).encode(buf);
            commands.put("NAMESPACE USE", buf);
        }

        for (Map.Entry<String, ByteBuf> entry : commands.entrySet()) {
            EmbeddedChannel channel = getChannel();
            Object response = runCommand(channel, entry.getValue());
            if (response instanceof ErrorRedisMessage actualMessage) {
                assertEquals(String.format("ERR Namespace '%s' is reserved for internal use", "name.__internal__"), actualMessage.content());
            } else {
                fail(String.format("Unexpected response: %s - %s", entry.getKey(), response));
            }
        }
    }

    @Test
    void test_CREATE_NAMESPACEALREADYEXISTS() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        {
            // Create it
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals(String.format("NAMESPACEALREADYEXISTS Namespace already exists: %s", namespace), actualMessage.content());
        }
    }

    @Test
    void test_EXISTS_NotExists() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceExists(namespace).encode(buf);

        EmbeddedChannel channel = getChannel();
        Object response = runCommand(channel, buf);
        assertInstanceOf(IntegerRedisMessage.class, response);
        IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
        assertEquals(0, actualMessage.value());
    }

    @Test
    void test_EXISTS() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        {
            // Create it
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceExists(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
            assertEquals(1, actualMessage.value());
        }
    }

    @Test
    void test_LIST() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceList(null).encode(buf);

        EmbeddedChannel channel = getChannel();

        Object response = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;
        assertEquals(1, actualMessage.children().size());
        SimpleStringRedisMessage item = (SimpleStringRedisMessage) actualMessage.children().getFirst();
        String namespace = instance.getContext().getConfig().getString("default_namespace");
        assertEquals(namespace, item.content());
    }

    @Test
    public void test_LIST_CREATE() {
        String root = UUID.randomUUID().toString();
        namespace = root;
        String sub = UUID.randomUUID().toString();
        String subnamespace = String.format("%s.%s", root, sub);
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        {
            // Create it
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(subnamespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceList(root).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;
            assertEquals(1, actualMessage.children().size());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceList(subnamespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;
            assertEquals(0, actualMessage.children().size());
        }
    }

    @Test
    void test_REMOVE_NOSUCHNAMESPACE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceRemove(namespace).encode(buf);
        EmbeddedChannel channel = getChannel();

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertEquals(String.format("NOSUCHNAMESPACE No such namespace: '%s'", namespace), actualMessage.content());
    }

    @Test
    void test_REMOVE_cannot_remove_default_namespace() {
        String defaultNamespace = instance.getContext().getConfig().getString("default_namespace");
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceRemove(defaultNamespace).encode(buf);
        EmbeddedChannel channel = getChannel();

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertEquals(String.format("ERR Cannot remove the default namespace: '%s'", defaultNamespace), actualMessage.content());
    }

    @Test
    void test_CREATE_then_REMOVE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        {
            // Create it
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceExists(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
            assertEquals(0, actualMessage.value());
        }
    }

    @Test
    void test_PrefixUtils_isPrefixStale() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        String name = "one.two.three";

        {
            // Create it
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(name).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        Prefix prefix;
        {
            ChannelHandlerContext ctx = new MockChannelHandlerContext(channel);
            Session.registerSession(context, ctx);
            Session session = Session.extractSessionFromChannel(channel);
            session.attr(SessionAttributes.CURRENT_NAMESPACE).set(name);

            BucketMetadata subspace = BucketMetadataUtil.createOrOpen(context, session, "test-bucket");
            prefix = subspace.volumePrefix();
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove("one").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            assertTrue(PrefixUtil.isStale(instance.getContext(), tr, prefix));
        }
    }

    @Test
    void test_MOVE() {
        String namespaceOld = UUID.randomUUID().toString();
        namespace = namespaceOld;
        String namespaceNew = UUID.randomUUID().toString();
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        {
            // Create it
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespaceOld).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
            namespace = namespaceOld;
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceMove(namespaceOld, namespaceNew).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
            namespace = namespaceNew;
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceExists(namespaceNew).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
            assertEquals(1, actualMessage.value());
        }
    }

    @Test
    void test_CURRENT() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            EmbeddedChannel channel = getChannel();

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCurrent().encode(buf);

            EmbeddedChannel channel = getChannel();

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(namespace, actualMessage.content());
        }
    }

    @Test
    void test_USE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        {
            // Create it
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }
    }

    @Test
    void test_USE_when_NOSUCHNAMESPACE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse("non.existing.namespace").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals("NOSUCHNAMESPACE No such namespace: 'non.existing.namespace'", actualMessage.content());
        }
    }
}
