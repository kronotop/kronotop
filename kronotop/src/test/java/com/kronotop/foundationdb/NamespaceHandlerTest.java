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

package com.kronotop.foundationdb;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.resp.ChannelAttributes;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.*;

public class NamespaceHandlerTest extends BaseHandlerTest {
    @Test
    public void testNamespaceCreate() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceCreate(namespace, null).encode(buf);

        EmbeddedChannel channel = getChannel();

        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        assertInstanceOf(SimpleStringRedisMessage.class, response);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
        assertEquals("OK", actualMessage.content());
    }

    @Test
    public void testNamespaceCreate_NAMESPACEALREADYEXISTS() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        {
            // Create it
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace, null).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace, null).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals(String.format("NAMESPACEALREADYEXISTS Namespace already exists: %s", namespace), actualMessage.content());
        }
    }

    @Test
    public void testNamespaceCreateOrOpen() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceCreateOrOpen(namespace, null).encode(buf);

        EmbeddedChannel channel = getChannel();

        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        assertInstanceOf(SimpleStringRedisMessage.class, response);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
        assertEquals("OK", actualMessage.content());
    }

    @Test
    public void testNamespaceCreateOrOpen_OpenExistingNamespace() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        {
            // Create it
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace, null).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreateOrOpen(namespace, null).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }
    }

    @Test
    public void testNamespaceExists_NotExists() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceExists(namespace).encode(buf);

        EmbeddedChannel channel = getChannel();

        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        assertInstanceOf(IntegerRedisMessage.class, response);
        IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
        assertEquals(0, actualMessage.value());
    }

    @Test
    public void testNamespaceExists() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        {
            // Create it
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace, null).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceExists(namespace).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
            assertEquals(1, actualMessage.value());
        }
    }

    @Test
    public void testNamespaceOpen_NOSUCHNAMESPACE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceOpen(namespace).encode(buf);

        EmbeddedChannel channel = getChannel();

        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertEquals(String.format("NOSUCHNAMESPACE No such namespace: %s", namespace), actualMessage.content());
    }

    @Test
    public void testNamespaceOpen() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        {
            // Create it
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace, null).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceOpen(namespace).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }
    }

    @Test
    public void testNamespaceList() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceList(null).encode(buf);

        EmbeddedChannel channel = getChannel();
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        assertInstanceOf(ArrayRedisMessage.class, response);
        // TODO: When we start using testcontainers or similar, comment out the following block.
        // ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;
        // assertEquals(0, actualMessage.children().size());
    }

    @Test
    public void testNamespaceList_Create() {
        String root = UUID.randomUUID().toString();
        namespace = root;
        String sub = UUID.randomUUID().toString();
        String subnamespace = String.format("%s.%s", root, sub);
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        {
            // Create it
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(subnamespace, null).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceList(root).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;
            assertEquals(1, actualMessage.children().size());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceList(subnamespace).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;
            assertEquals(0, actualMessage.children().size());
        }
    }

    @Test
    public void testNamespaceRemove_NOSUCHNAMESPACE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceRemove(namespace).encode(buf);
        EmbeddedChannel channel = getChannel();
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertEquals(String.format("NOSUCHNAMESPACE No such namespace: %s", namespace), actualMessage.content());
    }

    @Test
    public void testNamespaceCreate_Then_Remove() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        {
            // Create it
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace, null).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(namespace).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceExists(namespace).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
            assertEquals(0, actualMessage.value());
        }
    }

    @Test
    public void testNamespaceMove() {
        String namespaceOld = UUID.randomUUID().toString();
        namespace = namespaceOld;
        String namespaceNew = UUID.randomUUID().toString();
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        {
            // Create it
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreateOrOpen(namespaceOld, null).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
            namespace = namespaceOld;
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceMove(namespaceOld, namespaceNew).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
            namespace = namespaceNew;
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceExists(namespaceNew).encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
            assertEquals(1, actualMessage.value());
        }

        {
            // Check the cache
            ConcurrentMap<String, DirectorySubspace> openNamespaces = channel.attr(ChannelAttributes.OPEN_NAMESPACES).get();
            assertNull(openNamespaces.get(namespaceOld));
            assertNotNull(openNamespaces.get(namespaceNew));

            ConcurrentMap<String, Subspace> zmapSubspaces = channel.attr(ChannelAttributes.ZMAP_SUBSPACES).get();
            assertNull(zmapSubspaces.get(namespaceOld));
            assertNotNull(zmapSubspaces.get(namespaceNew));
        }
    }
}
