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

package com.kronotop.bucket;

import com.kronotop.bucket.pipeline.BasePipelineTest;
import com.kronotop.bucket.pipeline.PipelineNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class PlanCacheTTLExpirationTest extends BasePipelineTest {

    @Override
    protected String getConfigFileName() {
        return "test-plan-cache-short-ttl.conf";
    }

    @Test
    void shouldReturnNewPlanAfterTTLExpiration() throws InterruptedException {
        // Behavior: Cached plan should not be returned after TTL expiration
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // First query - creates and caches the plan
        PlanWithParams first = createPlanWithParams(metadata, "{'name': 'Alice'}");
        PipelineNode plan1 = first.plan();

        // The second query immediately - should return the cached plan (same identity)
        PlanWithParams second = createPlanWithParams(metadata, "{'name': 'Alice'}");
        PipelineNode plan2 = second.plan();
        assertSame(plan1, plan2, "Plan should be cached and returned immediately");

        // Wait for TTL to expire (max_ttl is 20ms, wait 100ms to be safe)
        Thread.sleep(100);

        // The third query after TTL - should create a new plan
        PlanWithParams third = createPlanWithParams(metadata, "{'name': 'Alice'}");
        PipelineNode plan3 = third.plan();
        assertNotSame(plan1, plan3, "New plan should be created after TTL expiration");
    }
}
