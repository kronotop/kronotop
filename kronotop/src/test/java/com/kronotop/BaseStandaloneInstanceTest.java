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

package com.kronotop;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketMetadataVersionBarrier;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.maintenance.IndexBuildingTask;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.server.MockChannelHandlerContext;
import com.kronotop.server.Session;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.transaction.TransactionUtil;
import com.typesafe.config.Config;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class BaseStandaloneInstanceTest extends BaseTest {
    private static final String DEFAULT_CONFIG_FILE_NAME = "test.conf";
    protected KronotopTestInstance instance;
    protected Config config;
    protected Context context; // shortcut

    protected String TEST_BUCKET = "test-bucket";
    protected String TEST_NAMESPACE;
    protected int TEST_SHARD_ID = 1;

    protected String getConfigFileName() {
        return DEFAULT_CONFIG_FILE_NAME;
    }

    protected DirectorySubspace createOrOpenSubspaceUnderCluster(String subspaceName) {
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            String clusterName = config.getString("cluster.name");
            List<String> subpath = KronotopDirectory.kronotop().cluster(clusterName).extend(subspaceName);
            DirectorySubspace subspace = instance.getContext().getDirectoryLayer().createOrOpen(tr, subpath).join();
            tr.commit().join();
            return subspace;
        }
    }

    protected Session getSession() {
        MockChannelHandlerContext ctx = new MockChannelHandlerContext(instance.getChannel());
        Session.registerSession(context, ctx);
        return Session.extractSessionFromChannel(ctx.channel());
    }

    protected void createBucket(String bucket, List<Integer> shards, String indexes) {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();

        cmd.create(bucket, BucketCreateArgs.Builder.shards(shards).indexes(indexes).ifNotExists()).encode(buf);
        Object response = runCommand(instance.getChannel(), buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
    }

    protected void createBucket(String bucket) {
        createBucket(bucket, List.of(TEST_SHARD_ID), null);
    }

    protected void createBucket(EmbeddedChannel channel, String namespace, String bucket) {
        // Switch to the target namespace
        KronotopCommandBuilder<String, String> nsCmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf useBuf = Unpooled.buffer();
        nsCmd.namespaceUse(namespace).encode(useBuf);
        Object useResponse = runCommand(channel, useBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, useResponse);

        // Create the bucket
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.create(bucket, BucketCreateArgs.Builder.shards(List.of(TEST_SHARD_ID)).ifNotExists()).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
    }

    protected BucketMetadata getBucketMetadata(String name) {
        return getBucketMetadata(TEST_NAMESPACE, name);
    }

    protected BucketMetadata getBucketMetadata(String namespace, String bucket) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return BucketMetadataUtil.open(context, tr, namespace, bucket);
        }
    }

    protected BucketMetadata reloadBucketMetadata(String namespace, String bucket) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return BucketMetadataUtil.reload(context, tr, namespace, bucket);
        }
    }

    @BeforeEach
    public void setup() throws UnknownHostException, InterruptedException {
        config = loadConfig(getConfigFileName());
        TEST_NAMESPACE = config.getString("default_namespace");
        instance = new KronotopTestInstance(config);
        instance.start();
        context = instance.getContext();
    }

    @AfterEach
    public void tearDown() {
        if (instance == null) {
            return;
        }
        instance.shutdown();
    }

    /**
     * Refreshes and retrieves the updated metadata of a specified bucket within the provided namespace.
     * This involves fetching the current metadata, ensuring it is up-to-date, and reopening it
     * with the latest information available.
     *
     * @param namespace the namespace to which the bucket belongs.
     * @param bucket    the name of the bucket whose metadata is being refreshed.
     * @return the updated {@code BucketMetadata} for the specified bucket.
     */
    protected BucketMetadata refreshBucketMetadata(String namespace, String bucket) {
        BucketMetadata metadata = getBucketMetadata(bucket);
        waitUntilUpdated(metadata);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return BucketMetadataUtil.reload(context, tr, namespace, bucket);
        }
    }

    /**
     * Waits until the provided bucket metadata is updated to the target version. This function ensures that
     * the version of the bucket metadata reaches the desired state by using a version barrier.
     *
     * @param metadata the {@code BucketMetadata} object representing the bucket whose metadata version
     *                 is being monitored for updates.
     */
    protected void waitUntilUpdated(BucketMetadata metadata) {
        long targetVersion = TransactionUtil.execute(context, tr -> BucketMetadataUtil.readVersion(tr, metadata.subspace()));
        BucketMetadataVersionBarrier barrier = new BucketMetadataVersionBarrier(context, metadata);
        barrier.await(targetVersion, 30, Duration.ofMillis(500));
    }

    protected void createIndexThenWaitForReadiness(CompoundIndexDefinition... definitions) {
        createIndexThenWaitForReadiness(TEST_NAMESPACE, TEST_BUCKET, definitions);
    }

    protected void waitForCompoundIndexReadiness(DirectorySubspace subspace) {
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                CompoundIndexDefinition definition = CompoundIndexUtil.loadIndexDefinition(tr, subspace);
                return definition.status() == IndexStatus.READY;
            }
        });
    }

    protected void createIndexThenWaitForReadiness(String namespace, String bucket, CompoundIndexDefinition... definitions) {
        createBucket(bucket);
        BucketMetadata metadata = getBucketMetadata(namespace, bucket);

        List<DirectorySubspace> subspaces = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            for (CompoundIndexDefinition def : definitions) {
                DirectorySubspace subspace = CompoundIndexUtil.create(tx, namespace, bucket, def);
                subspaces.add(subspace);
            }
            tr.commit().join();
        }

        for (DirectorySubspace subspace : subspaces) {
            waitForCompoundIndexReadiness(subspace);
        }

        waitUntilUpdated(metadata);
    }

    /**
     * Creates one or more indexes defined by the given {@code IndexDefinition} objects and waits until each index reaches the
     * readiness state. This method first creates the specified indexes in the database and then ensures they are ready for operations
     * by polling their status.
     *
     * @param definitions one or more {@code IndexDefinition} objects that specify the indexes to be created. Each definition
     *                    contains structural and metadata information about the index.
     */
    protected void createIndexThenWaitForReadiness(SingleFieldIndexDefinition... definitions) {
        createIndexThenWaitForReadiness(TEST_NAMESPACE, TEST_BUCKET, definitions);
    }

    protected void waitForIndexReadiness(DirectorySubspace subspace) {
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                SingleFieldIndexDefinition definition = SingleFieldIndexUtil.loadIndexDefinition(tr, subspace);
                return definition.status() == IndexStatus.READY;
            }
        });
    }

    protected void createIndexThenWaitForReadiness(String namespace, String bucket, SingleFieldIndexDefinition... definitions) {
        createBucket(bucket);

        BucketMetadata metadata = getBucketMetadata(namespace, bucket);

        List<DirectorySubspace> subspaces = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            for (SingleFieldIndexDefinition definition : definitions) {
                DirectorySubspace subspace = SingleFieldIndexUtil.create(tx, namespace, bucket, definition);
                subspaces.add(subspace);
            }
            tr.commit().join();
        }

        for (DirectorySubspace subspace : subspaces) {
            waitForIndexReadiness(subspace);
        }

        waitUntilUpdated(metadata);
    }

    protected IndexBuildingTask createIndexBuildingTask(long indexId) {
        return new IndexBuildingTask(
                TEST_NAMESPACE,
                TEST_BUCKET,
                indexId,
                TEST_SHARD_ID,
                TestUtil.generateVersionstamp(1),
                TestUtil.generateVersionstamp(2)
        );
    }
}
