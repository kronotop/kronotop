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
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.directory.KronotopDirectory;
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
     * Creates one or more indexes defined by the given {@code IndexDefinition} objects and waits until each index reaches the
     * readiness state. This method first creates the specified indexes in the database and then ensures they are ready for operations
     * by polling their status.
     *
     * @param definitions one or more {@code IndexDefinition} objects that specify the indexes to be created. Each definition
     *                    contains structural and metadata information about the index.
     */
    protected void createIndexThenWaitForReadiness(IndexDefinition... definitions) {
        Session session = getSession();
        BucketMetadataUtil.createOrOpen(context, session, TEST_BUCKET);

        List<DirectorySubspace> subspaces = new ArrayList<>();
        int userVersion = 0;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (IndexDefinition definition : definitions) {
                DirectorySubspace subspace = IndexUtil.create(context, tr, TEST_NAMESPACE, TEST_BUCKET, definition, userVersion);
                subspaces.add(subspace);
                userVersion++;
            }
            tr.commit().join();
        }

        for (DirectorySubspace subspace : subspaces) {
            await().atMost(Duration.ofSeconds(5)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    IndexDefinition definition = IndexUtil.loadIndexDefinition(tr, subspace);
                    return definition.status() == IndexStatus.READY;
                }
            });
        }
    }
}
