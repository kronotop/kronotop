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

package com.kronotop.namespace;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseHandlerTest;
import com.kronotop.TestUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.PlanCache;
import com.kronotop.bucket.index.maintenance.IndexBuildingTask;
import com.kronotop.bucket.index.maintenance.IndexTaskUtil;
import com.kronotop.bucket.pipeline.PipelineNode;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.StringUtil;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.namespace.handlers.Namespace;
import com.kronotop.namespace.handlers.NamespaceMetadata;
import com.kronotop.namespace.handlers.NamespaceRemovedEvent;
import com.kronotop.server.Response;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.PrefixUtil;
import com.kronotop.worker.Worker;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
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
        FullBulkStringRedisMessage item = (FullBulkStringRedisMessage) actualMessage.children().getFirst();
        String namespace = instance.getContext().getConfig().getString("default_namespace");
        assertEquals(namespace, item.content().toString(StandardCharsets.UTF_8));
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
            KronotopCommandBuilder<String, String> nsCmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
            ByteBuf useBuf = Unpooled.buffer();
            nsCmd.namespaceUse(name).encode(useBuf);
            runCommand(channel, useBuf);

            BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
            ByteBuf createBuf = Unpooled.buffer();
            bucketCmd.create(TEST_BUCKET).encode(createBuf);
            runCommand(channel, createBuf);

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, name, TEST_BUCKET);
                prefix = metadata.prefix();
            }
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
    void shouldResolveNamespacePathAfterMoveCommand() {
        // Behavior: After NAMESPACE MOVE within same parent, resolveNamespacePath returns the new path
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate("mvh.same.old").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceMove("mvh.same.old", "mvh.same.renamed").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = NamespaceUtil.open(tr, context, "mvh.same.renamed");
            List<String> subpath = new ArrayList<>();
            NamespaceUtil.resolveNamespacePath(tr, new Subspace(subspace.pack()), subpath);
            assertEquals("mvh.same.renamed", String.join(".", subpath.reversed()));
        }
    }

    @Test
    void shouldResolveNamespacePathAfterMoveCommandToDifferentParent() {
        // Behavior: After NAMESPACE MOVE to a different parent, resolveNamespacePath returns the new path
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate("mvh.src.child").encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate("mvh.dst").encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceMove("mvh.src.child", "mvh.dst.child").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = NamespaceUtil.open(tr, context, "mvh.dst.child");
            List<String> subpath = new ArrayList<>();
            NamespaceUtil.resolveNamespacePath(tr, new Subspace(subspace.pack()), subpath);
            assertEquals("mvh.dst.child", String.join(".", subpath.reversed()));
        }
    }

    @Test
    void shouldResolveNamespacePathAfterMoveCommandToRoot() {
        // Behavior: After NAMESPACE MOVE to root level, resolveNamespacePath returns the root name (PARENT_POINTER cleared)
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate("mvh.parent.child").encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceMove("mvh.parent.child", "mvhpromoted").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = NamespaceUtil.open(tr, context, "mvhpromoted");
            List<String> subpath = new ArrayList<>();
            NamespaceUtil.resolveNamespacePath(tr, new Subspace(subspace.pack()), subpath);
            assertEquals("mvhpromoted", String.join(".", subpath.reversed()));
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
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
            assertEquals(namespace, actualMessage.content().toString(StandardCharsets.UTF_8));
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

        {
            // Create namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Set bucket metadata in cache
        createBucket(TEST_BUCKET);
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        context.getBucketMetadataCache().set(namespace, TEST_BUCKET, metadata);
        assertNotNull(context.getBucketMetadataCache().get(namespace, TEST_BUCKET));

        {
            // Remove namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Wait for the journal event to be processed and cache to be invalidated
        await().atMost(15, TimeUnit.SECONDS).until(() ->
                context.getBucketMetadataCache().get(namespace, TEST_BUCKET) == null
        );
    }

    @Test
    void shouldInvalidateChildNamespacesCacheOnParentNamespaceRemoval() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();
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
        createBucket(TEST_BUCKET);
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        context.getBucketMetadataCache().set(childNamespace, TEST_BUCKET, metadata);
        assertNotNull(context.getBucketMetadataCache().get(childNamespace, TEST_BUCKET));

        {
            // Remove parent namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(parentNamespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Wait for the journal event to be processed and child namespace cache to be invalidated
        await().atMost(15, TimeUnit.SECONDS).until(() ->
                context.getBucketMetadataCache().get(childNamespace, TEST_BUCKET) == null
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
            assertTrue(actualMessage.content().startsWith("BARRIERNOTSATISFIED Barrier not satisfied"));
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
            DirectorySubspace memberSubspace = context.getDirectoryLayer().open(tr, memberSubpath).join();
            NamespaceUtil.setLastSeenNamespaceVersion(tr, memberSubspace, context, event);
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
        assertFalse(NamespaceUtil.exists(context, List.of(StringUtil.split(namespace))));
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
        UUID namespaceId = metadata.id();

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
                DirectorySubspace memberSubspace = context.getDirectoryLayer().open(tr, subpath).join();
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
        assertFalse(NamespaceUtil.exists(context, List.of(StringUtil.split(namespace))));
    }

    @Test
    void shouldCleanupOrphanedTasksWhenParentNamespaceIsPurged() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        String childNamespace = "a.b.c.d";
        String parentNamespace = "a.b";
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
            KronotopCommandBuilder<String, String> nsCmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
            ByteBuf useBuf = Unpooled.buffer();
            nsCmd.namespaceUse(childNamespace).encode(useBuf);
            runCommand(channel, useBuf);

            BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
            ByteBuf createBuf = Unpooled.buffer();
            bucketCmd.create(TEST_BUCKET).encode(createBuf);
            runCommand(channel, createBuf);
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
            DirectorySubspace memberSubspace = context.getDirectoryLayer().open(tr, memberSubpath).join();
            NamespaceUtil.setLastSeenNamespaceVersion(tr, memberSubspace, context, event);
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
                TEST_BUCKET,
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

    @Test
    void shouldInvalidatePlanCacheOnNamespaceRemovedEvent() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            // Create namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Get a plan cache and populate it with a dummy plan entry
        BucketService bucketService = context.getService(BucketService.NAME);
        PlanCache planCache = bucketService.getPlanCache();

        UUID bucketId = UUID.fromString("00000000-0000-0000-0000-00000000007b");
        long shapeHash = 456L;
        PipelineNode dummyPlan = new PipelineNode() {
            @Override
            public int id() {
                return 1;
            }

            @Override
            public PipelineNode next() {
                return null;
            }

            @Override
            public void connectNext(PipelineNode node) {
            }
        };

        planCache.put(namespace, bucketId, shapeHash, dummyPlan);
        assertNotNull(planCache.get(namespace, bucketId, shapeHash), "Plan should be cached");

        {
            // Remove namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceRemove(namespace).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Wait for the journal event to be processed and plan cache to be invalidated
        await().atMost(15, TimeUnit.SECONDS).until(() ->
                planCache.get(namespace, bucketId, shapeHash) == null
        );
    }

    @Test
    void shouldInvalidateBucketMetadataCacheOnNamespaceMove() {
        // Behavior: After a NAMESPACE MOVE, the BucketMetadataCache entry keyed under the old namespace path is invalidated.
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

        // Set bucket metadata in cache
        createBucket(TEST_BUCKET);
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        context.getBucketMetadataCache().set(namespaceOld, TEST_BUCKET, metadata);
        assertNotNull(context.getBucketMetadataCache().get(namespaceOld, TEST_BUCKET));

        {
            // Move namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceMove(namespaceOld, namespaceNew).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            namespace = namespaceNew;
        }

        // Wait for the journal event to be processed and cache to be invalidated
        await().atMost(15, TimeUnit.SECONDS).until(() ->
                context.getBucketMetadataCache().get(namespaceOld, TEST_BUCKET) == null
        );
    }

    @Test
    void shouldInvalidatePlanCacheOnNamespaceMove() {
        // Behavior: After a NAMESPACE MOVE, the PlanCache entries keyed under the old namespace path are invalidated.
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

        // Get a plan cache and populate it with a dummy plan entry
        BucketService bucketService = context.getService(BucketService.NAME);
        PlanCache planCache = bucketService.getPlanCache();

        UUID bucketId = UUID.fromString("00000000-0000-0000-0000-00000000007b");
        long shapeHash = 456L;
        PipelineNode dummyPlan = new PipelineNode() {
            @Override
            public int id() {
                return 1;
            }

            @Override
            public PipelineNode next() {
                return null;
            }

            @Override
            public void connectNext(PipelineNode node) {
            }
        };

        planCache.put(namespaceOld, bucketId, shapeHash, dummyPlan);
        assertNotNull(planCache.get(namespaceOld, bucketId, shapeHash), "Plan should be cached");

        {
            // Move namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceMove(namespaceOld, namespaceNew).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            namespace = namespaceNew;
        }

        // Wait for the journal event to be processed and plan cache to be invalidated
        await().atMost(15, TimeUnit.SECONDS).until(() ->
                planCache.get(namespaceOld, bucketId, shapeHash) == null
        );
    }

    @Test
    void shouldBlockCreateWhenMoveTombstoneNotObserved() {
        // Behavior: After a NAMESPACE MOVE, CREATE on the old name is rejected until all cluster members have observed the tombstone.
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
            // Move namespace — sets a tombstone on the old name
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceMove(namespaceOld, namespaceNew).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            namespace = namespaceNew;
        }

        // Clear all tombstone data (token + observers), then re-set just the token.
        // This leaves the tombstone present but with zero observers, so the barrier is unsatisfied.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TombstoneManager.cleanup(context, tr, namespaceOld);
            TombstoneManager.setTombstone(context, tr, namespaceOld);
            tr.commit().join();
        }

        {
            // CREATE on the old name must fail — the tombstone barrier is not satisfied
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespaceOld).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertTrue(actualMessage.content().startsWith("BARRIERNOTSATISFIED"));
            assertTrue(actualMessage.content().contains("Not all cluster members have observed the tombstone"));
        }
    }

    @Test
    void shouldInvalidateOpenNamespacesOnNamespaceMove() {
        // Behavior: After a NAMESPACE MOVE, the old namespace is removed from all sessions' OPEN_NAMESPACES.
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

        // Get session and add namespace to OPEN_NAMESPACES
        Session session = getSession();
        Map<String, Namespace> openNamespaces = session.attr(SessionAttributes.OPEN_NAMESPACES).get();
        openNamespaces.put(namespaceOld, new Namespace());

        // Register session in SessionStore
        context.getSessionStore().put(session.getClientId(), session);
        assertTrue(openNamespaces.containsKey(namespaceOld));

        {
            // Move namespace
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceMove(namespaceOld, namespaceNew).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            namespace = namespaceNew;
        }

        // Wait for the journal event to be processed and OPEN_NAMESPACES to be invalidated
        await().atMost(15, TimeUnit.SECONDS).until(() ->
                !openNamespaces.containsKey(namespaceOld)
        );
    }
}
