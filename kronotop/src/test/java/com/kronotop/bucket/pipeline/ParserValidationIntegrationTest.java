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

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.bql.BqlParseException;
import com.kronotop.bucket.index.IndexDefinition;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ParserValidationIntegrationTest extends BasePipelineTest {

    @Test
    void shouldRejectNullValueForComparisonOperators() {
        final String TEST_BUCKET_NAME = "test-bucket-reject-null-comparison";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);
        assertThrows(BqlParseException.class, () -> createExecutionPlan(metadata, "{'age': {'$gt': null}}"));
        assertThrows(BqlParseException.class, () -> createExecutionPlan(metadata, "{'age': {'$gte': null}}"));
        assertThrows(BqlParseException.class, () -> createExecutionPlan(metadata, "{'age': {'$lt': null}}"));
        assertThrows(BqlParseException.class, () -> createExecutionPlan(metadata, "{'age': {'$lte': null}}"));
    }

    @Test
    void shouldAcceptNullValueForSupportedOperators() {
        final String TEST_BUCKET_NAME = "test-bucket-accept-null-operators";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);
        assertDoesNotThrow(() -> createExecutionPlan(metadata, "{'age': {'$eq': null}}"));
        assertDoesNotThrow(() -> createExecutionPlan(metadata, "{'age': {'$ne': null}}"));
        assertDoesNotThrow(() -> createExecutionPlan(metadata, "{'age': {'$in': [null, 20]}}"));
        assertDoesNotThrow(() -> createExecutionPlan(metadata, "{'age': {'$nin': [null, 20]}}"));
        assertDoesNotThrow(() -> createExecutionPlan(metadata, "{'age': {'$all': [null, 20]}}"));
    }

    @Test
    void shouldRejectRangeScanWithNullBoundaries() {
        final String TEST_BUCKET_NAME = "test-bucket-rangescan-null-boundaries";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        assertThrows(BqlParseException.class, () ->
                createExecutionPlan(metadata, "{ 'age': { '$gt': null, '$lt': null } }"));
    }
}
