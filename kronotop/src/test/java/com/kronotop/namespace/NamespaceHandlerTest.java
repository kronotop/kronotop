/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.namespace;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseHandlerTest;
import com.kronotop.TestUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.namespace.handlers.Namespace;
import com.kronotop.namespace.handlers.NamespaceMetadata;
import com.kronotop.namespace.handlers.NamespaceRemovedEvent;
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
import com.kronotop.bucket.index.maintenance.IndexBuildingTask;
import com.kronotop.bucket.index.maintenance.IndexTaskUtil;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.worker.Worker;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class NamespaceHandlerTest extends BaseHandlerTest {

    @Test
    void shouldCreateNamespace() {
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
    void shouldRejectInternalNamespaceAccess() {
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
    void shouldThrowErrorWhenCreatingExistingNamespace() {
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
    void shouldThrowErrorWhenCreatingNamespaceBeingRemoved() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            // Create namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            // Remove namespace (marks it as being removed)
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            // Try to create the namespace that is being removed
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals(String.format("NAMESPACEBEINGREMOVED Namespace '%s' is being removed", namespace), actualMessage.content());
        }
    }

    @Test
    void shouldReturnZeroWhenNamespaceDoesNotExist() {
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
    void shouldReturnOneWhenNamespaceExists() {
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
    void shouldListDefaultNamespace() {
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
    void shouldListSubnamespaces() {
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
    void shouldThrowErrorWhenRemovingNonExistentNamespace() {
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
    void shouldThrowErrorWhenRemovingDefaultNamespace() {
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
    void shouldMarkNamespaceAsBeingRemovedAfterRemove() {
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
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals(String.format("NAMESPACEBEINGREMOVED Namespace '%s' is being removed", namespace), actualMessage.content());
        }
    }

    @Test
    void shouldThrowErrorWhenRemovingNamespaceBeingRemoved() {
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
            cmd.namespaceRemove(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals(String.format("NAMESPACEBEINGREMOVED Namespace '%s' is being removed", namespace), actualMessage.content());
        }
    }

    @Test
    void shouldMarkPrefixAsStaleAfterNamespaceRemoval() {
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

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespacePurge("one").encode(buf);

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
    void shouldMoveNamespace() {
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
    void shouldReturnCurrentNamespace() {
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
    void shouldSwitchToNamespace() {
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
    void shouldThrowErrorWhenUsingNonExistentNamespace() {
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

    @Test
    void shouldThrowErrorWhenMovingNamespaceBeingRemoved() {
        String namespaceOld = UUID.randomUUID().toString();
        namespace = namespaceOld;
        String namespaceNew = UUID.randomUUID().toString();
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            // Create namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespaceOld).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            // Remove namespace (marks it as being removed)
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(namespaceOld).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            // Try to move the namespace that is being removed
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceMove(namespaceOld, namespaceNew).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals(String.format("NAMESPACEBEINGREMOVED Namespace '%s' is being removed", namespaceOld), actualMessage.content());
        }
    }

    @Test
    void shouldThrowErrorWhenUsingNamespaceBeingRemoved() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            // Create namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            // Remove namespace (marks it as being removed)
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            // Try to use the namespace that is being removed
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals(String.format("NAMESPACEBEINGREMOVED Namespace '%s' is being removed", namespace), actualMessage.content());
        }
    }

    @Test
    void shouldInvalidateBucketMetadataCacheOnNamespaceRemoval() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        String bucket = "test-bucket";

        {
            // Create namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Set bucket metadata in cache
        BucketMetadata metadata = getBucketMetadata(bucket);
        context.getBucketMetadataCache().set(namespace, bucket, metadata);
        assertNotNull(context.getBucketMetadataCache().get(namespace, bucket));

        {
            // Remove namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Wait for the journal event to be processed and cache to be invalidated
        await().atMost(15, TimeUnit.SECONDS).until(() ->
                context.getBucketMetadataCache().get(namespace, bucket) == null
        );
    }

    @Test
    void shouldInvalidateChildNamespacesCacheOnParentNamespaceRemoval() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
        String bucket = "test-bucket";
        String parentNamespace = "a.b";
        String childNamespace = "a.b.c.d";
        namespace = parentNamespace;

        {
            // Create parent namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(parentNamespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Set bucket metadata in cache for child namespace
        BucketMetadata metadata = getBucketMetadata(bucket);
        context.getBucketMetadataCache().set(childNamespace, bucket, metadata);
        assertNotNull(context.getBucketMetadataCache().get(childNamespace, bucket));

        {
            // Remove parent namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(parentNamespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Wait for the journal event to be processed and child namespace cache to be invalidated
        await().atMost(15, TimeUnit.SECONDS).until(() ->
                context.getBucketMetadataCache().get(childNamespace, bucket) == null
        );
    }

    @Test
    void shouldInvalidateOpenNamespacesOnNamespaceRemoval() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            // Create namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Get session and add namespace to OPEN_NAMESPACES
        Session session = getSession();
        Map<String, Namespace> openNamespaces = session.attr(SessionAttributes.OPEN_NAMESPACES).get();
        openNamespaces.put(namespace, new Namespace());

        // Register session in SessionStore
        context.getSessionStore().put(session.getClientId(), session);
        assertTrue(openNamespaces.containsKey(namespace));

        {
            // Remove namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Wait for the journal event to be processed and OPEN_NAMESPACES to be invalidated
        await().atMost(15, TimeUnit.SECONDS).until(() ->
                !openNamespaces.containsKey(namespace)
        );
    }

    @Test
    void shouldThrowErrorWhenPurgingDefaultNamespace() {
        String defaultNamespace = instance.getContext().getConfig().getString("default_namespace");
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.namespacePurge(defaultNamespace).encode(buf);
        EmbeddedChannel channel = getChannel();

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertEquals(String.format("ERR Cannot purge the default namespace: '%s'", defaultNamespace), actualMessage.content());
    }

    @Test
    void shouldThrowErrorWhenPurgingNamespaceNotMarkedAsRemoved() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            // Create namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            // Try to purge without removing first
            ByteBuf buf = Unpooled.buffer();
            cmd.namespacePurge(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals(String.format("ERR Namespace '%s' must be logically removed before purge", namespace), actualMessage.content());
        }
    }

    @Test
    void shouldThrowBarrierNotSatisfiedWhenMembersHaveNotObservedVersion() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            // Create namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Directly mark as removed via NamespaceUtil to avoid triggering journal event processing
        NamespaceUtil.setRemoved(context, namespace);

        {
            // Try to purge - barrier should not be satisfied since no member has observed the version
            ByteBuf buf = Unpooled.buffer();
            cmd.namespacePurge(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertTrue(actualMessage.content().startsWith("ERR Barrier not satisfied"));
        }
    }

    @Test
    void shouldPurgeNamespaceSuccessfully() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            // Create namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Directly mark as removed via NamespaceUtil
        NamespaceMetadata metadata = NamespaceUtil.readMetadata(context, namespace);
        NamespaceUtil.setRemoved(context, namespace);

        // Set lastSeenNamespaceVersion for this member to satisfy the barrier
        NamespaceRemovedEvent event = new NamespaceRemovedEvent(metadata.id(), namespace);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> memberSubpath = KronotopDirectory.
                    kronotop().
                    cluster(context.getClusterName()).
                    metadata().
                    members().
                    member(context.getMember().getId()).toList();
            DirectorySubspace memberSubspace = DirectoryLayer.getDefault().open(tr, memberSubpath).join();
            NamespaceUtil.setLastSeenNamespaceVersion(tr, memberSubspace, context.getClusterName(), event);
            tr.commit().join();
        }

        {
            // Purge should succeed
            ByteBuf buf = Unpooled.buffer();
            cmd.namespacePurge(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify namespace no longer exists
        assertFalse(NamespaceUtil.exists(context, List.of(namespace.split("\\."))));
    }

    @Test
    void shouldSetLastSeenNamespaceVersionOnNamespaceRemoval() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            // Create namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Get namespace ID before removal
        NamespaceMetadata metadata = NamespaceUtil.readMetadata(context, namespace);
        long namespaceId = metadata.id();

        {
            // Remove namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Wait for the journal event to be processed and the last seen version to be set
        await().atMost(15, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                List<String> subpath = KronotopDirectory.
                        kronotop().
                        cluster(context.getClusterName()).
                        metadata().
                        members().
                        member(context.getMember().getId()).toList();
                DirectorySubspace memberSubspace = DirectoryLayer.getDefault().open(tr, subpath).join();
                Long version = NamespaceUtil.readLastSeenNamespaceVersion(tr, memberSubspace, namespaceId);
                assertNotNull(version);
                return version == 1;
            }
        });
    }

    @Test
    void shouldShutdownWorkersAndPurgeNamespaceSuccessfully() {
        class TestWorker implements Worker {
            private final String workerNamespace;
            private volatile boolean shutdownCalled = false;

            TestWorker(String workerNamespace) {
                this.workerNamespace = workerNamespace;
            }

            @Override
            public String getTag() {
                return "test-worker";
            }

            @Override
            public void shutdown() {
                shutdownCalled = true;
            }

            @Override
            public boolean await(long timeout, TimeUnit unit) {
                // Remove self from registry (simulating completion hook)
                context.getWorkerRegistry().remove(workerNamespace, this);
                return true;
            }

            public boolean isShutdownCalled() {
                return shutdownCalled;
            }
        }

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            // Create namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Create and register a test worker
        TestWorker worker = new TestWorker(namespace);
        context.getWorkerRegistry().put(namespace, worker);

        // Verify worker is registered
        List<Worker> workers = context.getWorkerRegistry().get(namespace, "test-worker");
        assertEquals(1, workers.size());

        {
            // Remove namespace (triggers processNamespaceRemovedEvent via journal)
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Wait for the worker to be shut down and removed from registry
        await().atMost(15, TimeUnit.SECONDS).until(() -> {
            List<Worker> remaining = context.getWorkerRegistry().get(namespace, "test-worker");
            return remaining.isEmpty() && worker.isShutdownCalled();
        });

        {
            // Purge namespace - should succeed because barrier is satisfied
            ByteBuf buf = Unpooled.buffer();
            cmd.namespacePurge(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify namespace no longer exists
        assertFalse(NamespaceUtil.exists(context, List.of(namespace.split("\\."))));
    }

    @Test
    void shouldFailPurgeWhenWorkerDoesNotRemoveItself() {
        class StubbornWorker implements Worker {
            private volatile boolean shutdownCalled = false;

            @Override
            public String getTag() {
                return "stubborn-worker";
            }

            @Override
            public void shutdown() {
                shutdownCalled = true;
            }

            @Override
            public boolean await(long timeout, TimeUnit unit) {
                // Does NOT remove itself from registry (simulating a stuck worker)
                return true;
            }

            public boolean isShutdownCalled() {
                return shutdownCalled;
            }
        }

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            // Create namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Create and register a stubborn worker that won't remove itself
        StubbornWorker worker = new StubbornWorker();
        context.getWorkerRegistry().put(namespace, worker);

        // Verify worker is registered
        List<Worker> workers = context.getWorkerRegistry().get(namespace, "stubborn-worker");
        assertEquals(1, workers.size());

        {
            // Remove namespace (triggers processNamespaceRemovedEvent via journal)
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Wait for shutdown to be called, but worker stays in registry
        await().atMost(15, TimeUnit.SECONDS).until(worker::isShutdownCalled);

        // Worker should still be in registry
        List<Worker> remaining = context.getWorkerRegistry().get(namespace, "stubborn-worker");
        assertEquals(1, remaining.size());

        {
            // Purge namespace - should fail because barrier is not satisfied
            ByteBuf buf = Unpooled.buffer();
            cmd.namespacePurge(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertTrue(actualMessage.content().startsWith("ERR Barrier not satisfied"));
        }

        // Cleanup: remove worker from registry to allow test teardown
        context.getWorkerRegistry().remove(namespace, worker);
    }

    @Test
    void shouldCleanupOrphanedTasksWhenParentNamespaceIsPurged() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        String childNamespace = "a.b.c.d";
        String parentNamespace = "a.b";
        String bucket = "test-bucket";
        int shardId = 1;

        {
            // Create child namespace a.b.c.d
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(childNamespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            // Create a bucket in the child namespace
            ChannelHandlerContext ctx = new MockChannelHandlerContext(channel);
            Session.registerSession(context, ctx);
            Session session = Session.extractSessionFromChannel(channel);
            session.attr(SessionAttributes.CURRENT_NAMESPACE).set(childNamespace);

            BucketMetadataUtil.createOrOpen(context, session, bucket);
        }

        // Get namespace metadata for barrier setup
        NamespaceMetadata metadata = NamespaceUtil.readMetadata(context, parentNamespace);

        {
            // Remove the parent namespace a.b (marks it and all children as being removed)
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(parentNamespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Set the lastSeenNamespaceVersion for this member to satisfy the barrier
        NamespaceRemovedEvent event = new NamespaceRemovedEvent(metadata.id(), parentNamespace);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> memberSubpath = KronotopDirectory.
                    kronotop().
                    cluster(context.getClusterName()).
                    metadata().
                    members().
                    member(context.getMember().getId()).toList();
            DirectorySubspace memberSubspace = DirectoryLayer.getDefault().open(tr, memberSubpath).join();
            NamespaceUtil.setLastSeenNamespaceVersion(tr, memberSubspace, context.getClusterName(), event);
            tr.commit().join();
        }

        {
            // Purge the parent namespace a.b
            ByteBuf buf = Unpooled.buffer();
            cmd.namespacePurge(parentNamespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Namespace is purged. Now create an orphaned task that references the purged namespace/bucket.
        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, shardId);
        IndexBuildingTask orphanedTask = new IndexBuildingTask(
                childNamespace,
                bucket,
                1, // indexId
                shardId,
                TestUtil.generateVersionstamp(1),
                TestUtil.generateVersionstamp(2)
        );
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(orphanedTask));
        assertNotNull(taskId);

        // Verify the task exists
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, taskSubspace, taskId);
            assertNotNull(definition);
        }

        // Explicitly trigger the watchdog to wake up and process the task queue.
        // TaskStorage.create() already triggers watchers, but this ensures the
        // watchdog processes the queue immediately.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.triggerWatchers(tr, taskSubspace);
            tr.commit().join();
        }

        // The BucketService's watchdog will detect the orphaned task (bucket no longer exists) and drop it.
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                byte[] definition = TaskStorage.getDefinition(tr, taskSubspace, taskId);
                return definition == null; // Task should be dropped by GC
            }
        });
    }
}
