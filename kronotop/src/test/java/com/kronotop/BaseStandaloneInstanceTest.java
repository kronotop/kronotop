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
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.server.MockChannelHandlerContext;
import com.kronotop.server.Session;
import com.typesafe.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;

public class BaseStandaloneInstanceTest extends BaseTest {
    private final Random random = new Random(System.currentTimeMillis());
    private static final String DEFAULT_CONFIG_FILE_NAME = "test.conf";
    protected KronotopTestInstance instance;
    protected Config config;
    protected Context context; // shortcut

    protected String TEST_BUCKET_NAME = "test-bucket";

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
}
