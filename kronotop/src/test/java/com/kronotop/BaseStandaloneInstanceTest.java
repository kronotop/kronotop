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

package com.kronotop;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketMetadataVersionBarrier;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.MockChannelHandlerContext;
import com.kronotop.server.Session;
import com.typesafe.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.awaitility.Awaitility.await;

public class BaseStandaloneInstanceTest extends BaseTest {
    private static final String DEFAULT_CONFIG_FILE_NAME = "test.conf";
    private final Random random = new Random(System.currentTimeMillis());
    protected KronotopTestInstance instance;
    protected Config config;
    protected Context context; // shortcut

    protected String TEST_BUCKET = "test-bucket";
    protected String TEST_NAMESPACE;

    protected String getConfigFileName() {
        return DEFAULT_CONFIG_FILE_NAME;
    }

    protected DirectorySubspace createOrOpenSubspaceUnderCluster(String subspaceName) {
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            String clusterName = config.getString("cluster.name");
            List<String> subpath = KronotopDirectory.kronotop().cluster(clusterName).extend(subspaceName);
            DirectorySubspace subspace = DirectoryLayer.getDefault().createOrOpen(tr, subpath).join();
            tr.commit().join();
            return subspace;
        }
    }

    protected Session getSession() {
        MockChannelHandlerContext ctx = new MockChannelHandlerContext(instance.getChannel());
        Session.registerSession(context, ctx);
        return Session.extractSessionFromChannel(ctx.channel());
    }

    protected BucketMetadata getBucketMetadata(String name) {
        Session session = getSession();
        return BucketMetadataUtil.createOrOpen(context, session, name);
    }

    protected Versionstamp generateVersionstamp(int userVersion) {
        byte[] trVersion = new byte[10];
        random.nextBytes(trVersion);
        return Versionstamp.complete(trVersion, userVersion);
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
     * @param bucket the name of the bucket whose metadata is being refreshed.
     * @return the updated {@code BucketMetadata} for the specified bucket.
     */
    protected BucketMetadata refreshBucketMetadata(String namespace, String bucket) {
        BucketMetadata metadata = getBucketMetadata(bucket);
        waitUntilUpdated(metadata);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return BucketMetadataUtil.forceOpen(context, tr, namespace, bucket);
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
        long targetVersion = TransactionUtils.execute(context, tr -> BucketMetadataUtil.readVersion(tr, metadata.subspace()));
        BucketMetadataVersionBarrier barrier = new BucketMetadataVersionBarrier(context, metadata);
        barrier.await(targetVersion, 30, Duration.ofMillis(500));
    }

    /**
     * Creates one or more indexes defined by the given {@code IndexDefinition} objects and waits until each index reaches the
     * readiness state. This method first creates the specified indexes in the database and then ensures they are ready for operations
     * by polling their status.
     *
     * @param definitions one or more {@code IndexDefinition} objects that specify the indexes to be created. Each definition
     *                    contains structural and metadata information about the index.
     */
    protected void createIndexThenWaitForReadiness(IndexDefinition... definitions) {
        createIndexThenWaitForReadiness(TEST_NAMESPACE, TEST_BUCKET, definitions);
    }

    protected void waitForIndexReadiness(DirectorySubspace subspace) {
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexDefinition definition = IndexUtil.loadIndexDefinition(tr, subspace);
                return definition.status() == IndexStatus.READY;
            }
        });
    }

    protected void createIndexThenWaitForReadiness(String namespace, String bucket, IndexDefinition... definitions) {
        Session session = getSession();
        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, bucket);

        List<DirectorySubspace> subspaces = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            for (IndexDefinition definition : definitions) {
                DirectorySubspace subspace = IndexUtil.create(tx, namespace, bucket, definition);
                subspaces.add(subspace);
            }
            tr.commit().join();
        }

        for (DirectorySubspace subspace : subspaces) {
            waitForIndexReadiness(subspace);
        }

        waitUntilUpdated(metadata);
    }
}
