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

package com.kronotop.cluster;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultiClusterBucketIsolationTest extends BaseMultiClusterTest {

    @Test
    void shouldIsolateDataBetweenClusters() {
        // Behavior: Data written to Cluster-A is not visible from Cluster-B.

        createBucket(clusterA.getChannel());
        insertDocument(clusterA.getChannel(), "{\"source\": \"cluster-a\"}");

        List<String> bucketsOnB = listBuckets(clusterB.getChannel());
        assertTrue(bucketsOnB.isEmpty());
    }

    @Test
    void shouldSurviveDropOfAnotherCluster() {
        // Behavior: Dropping Cluster-A's FDB directory does not affect Cluster-B's data.

        createBucket(clusterA.getChannel());
        insertDocument(clusterA.getChannel(), "{\"source\": \"cluster-a\"}");

        createBucket(clusterB.getChannel());
        insertDocument(clusterB.getChannel(), "{\"source\": \"cluster-b\"}");

        // Drop Cluster-A via the two-step DROP-CLUSTER command
        dropCluster(clusterA);

        // Cluster-B should still have its bucket
        List<String> bucketsOnB = listBuckets(clusterB.getChannel());
        assertEquals(1, bucketsOnB.size());
        assertEquals(TEST_BUCKET, bucketsOnB.getFirst());

        // Cluster-B should still be able to insert and query
        insertDocument(clusterB.getChannel(), "{\"source\": \"cluster-b-after-drop\"}");

        switchToResp3(clusterB.getChannel());
        List<BsonDocument> entries = queryAll(clusterB.getChannel());
        assertEquals(2, entries.size());
    }
}
