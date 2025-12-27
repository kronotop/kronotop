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

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import org.bson.BsonType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class PipelineRewriterTest extends BasePipelineTest {

    @Test
    @Disabled("buggy")
    void shouldRewritePhysicalIndexIntersectionToIntersectionNode() {
        // Create two secondary indexes
        IndexDefinition ageIndex = IndexDefinition.create("age_idx", "age", BsonType.INT32);
        IndexDefinition nameIndex = IndexDefinition.create("name_idx", "name", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, ageIndex, nameIndex);

        // Insert documents with both fields
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 25}")
        );
        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        // Create execution plan with $and on two indexed fields
        String query = "{$and: [{age: {$eq: 25}}, {name: {$eq: \"Alice\"}}]}";
        PipelineNode plan = createExecutionPlan(metadata, query);

        // Verify the plan is an IntersectionNode
        assertNotNull(plan);
        assertInstanceOf(IntersectionNode.class, plan);

        IntersectionNode intersectionNode = (IntersectionNode) plan;
        assertEquals(2, intersectionNode.children().size());

        // Verify children are IndexScanNodes
        for (PipelineNode child : intersectionNode.children()) {
            assertInstanceOf(IndexScanNode.class, child);
        }

        // Run the query and verify the result
        List<String> results = runQueryOnBucket(metadata, query);
        assertEquals(1, results.size());

        // Verify the returned document is Alice with age 25
        String resultJson = results.getFirst();
        assertTrue(resultJson.contains("\"name\": \"Alice\""));
        assertTrue(resultJson.contains("\"age\": 25"));
    }
}
