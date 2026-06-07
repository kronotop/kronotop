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

package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.Collation;
import com.kronotop.bucket.index.CompoundIndexDefinition;
import com.kronotop.bucket.index.CompoundIndexField;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import org.bson.BsonType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class PhysicalPlannerCollationTest extends BasePhysicalPlannerTest {

    private static Collation collation(String locale) {
        return Collation.create(locale, 3, null, null, null, null, null, null, null);
    }

    private void createSingleFieldIndex(SingleFieldIndexDefinition definition) {
        createIndexThenWaitForReadiness(definition);
        metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
    }

    private void createCompoundIndex(CompoundIndexDefinition definition) {
        createIndexThenWaitForReadiness(definition);
        metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
    }

    @Nested
    @DisplayName("Single-field index collation compatibility")
    class SingleFieldIndexCollation {

        @Test
        void shouldSkipStringIndexWhenQueryCollationDiffers() {
            // Behavior: STRING index with collation "en" is skipped when query specifies collation "tr"
            createSingleFieldIndex(SingleFieldIndexDefinition.create(
                    "name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING, collation("en")));

            PhysicalNode result = planQuery("{ 'name': 'alice' }", null, collation("tr"));

            assertInstanceOf(PhysicalFullScan.class, result);
        }

        @Test
        void shouldUseStringIndexWhenQueryCollationMatches() {
            // Behavior: STRING index with collation "en" is used when query specifies the same collation
            createSingleFieldIndex(SingleFieldIndexDefinition.create(
                    "name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING, collation("en")));

            PhysicalNode result = planQuery("{ 'name': 'alice' }", null, collation("en"));

            assertInstanceOf(PhysicalIndexScan.class, result);
        }

        @Test
        void shouldUseStringIndexWhenNoQueryCollation() {
            // Behavior: STRING index is used normally when query specifies no collation (existing behavior preserved)
            createSingleFieldIndex(SingleFieldIndexDefinition.create(
                    "name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING, collation("en")));

            PhysicalNode result = planQuery("{ 'name': 'alice' }");

            assertInstanceOf(PhysicalIndexScan.class, result);
        }

        @Test
        void shouldNotSkipNonStringIndexWhenQueryCollationDiffers() {
            // Behavior: INT32 index is unaffected by query collation since collation is irrelevant for non-string types
            createSingleFieldIndex(SingleFieldIndexDefinition.create(
                    "age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ 'age': 25 }", null, collation("tr"));

            assertInstanceOf(PhysicalIndexScan.class, result);
        }

        @Test
        void shouldSkipStringIndexWithNullCollationWhenQueryHasCollation() {
            // Behavior: STRING index with binary ordering (null collation) cannot serve locale-aware queries
            createSingleFieldIndex(SingleFieldIndexDefinition.create(
                    "name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ 'name': 'alice' }", null, collation("en"));

            assertInstanceOf(PhysicalFullScan.class, result);
        }

        @Test
        void shouldSkipStringIndexForInOperatorWhenCollationDiffers() {
            // Behavior: $in optimization is skipped when index collation doesn't match query collation
            createSingleFieldIndex(SingleFieldIndexDefinition.create(
                    "name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING, collation("en")));

            PhysicalNode result = planQuery("{ 'name': { '$in': ['alice', 'bob'] } }", null, collation("tr"));

            assertInstanceOf(PhysicalFullScan.class, result);
        }
    }

    @Nested
    @DisplayName("Compound index collation compatibility")
    class CompoundIndexCollation {

        @Test
        void shouldSkipCompoundIndexWithStringFieldWhenCollationDiffers() {
            // Behavior: compound index with STRING field is skipped when query collation doesn't match;
            //           two-predicate query falls back to a PhysicalAnd of full scans
            createCompoundIndex(CompoundIndexDefinition.create("name_age_idx", List.of(
                    new CompoundIndexField("name", BsonType.STRING, false),
                    new CompoundIndexField("age", BsonType.INT32, false)
            ), IndexStatus.WAITING, collation("en")));

            PhysicalNode result = planQuery("{ 'name': 'alice', 'age': 25 }", null, collation("tr"));

            assertInstanceOf(PhysicalAnd.class, result);
        }

        @Test
        void shouldUseCompoundIndexWithStringFieldWhenCollationMatches() {
            // Behavior: compound index with STRING field is used when query collation matches
            createCompoundIndex(CompoundIndexDefinition.create("name_age_idx", List.of(
                    new CompoundIndexField("name", BsonType.STRING, false),
                    new CompoundIndexField("age", BsonType.INT32, false)
            ), IndexStatus.WAITING, collation("en")));

            PhysicalNode result = planQuery("{ 'name': 'alice', 'age': 25 }", null, collation("en"));

            assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        }

        @Test
        void shouldNotSkipPurelyNonStringCompoundIndexWhenCollationDiffers() {
            // Behavior: compound index with only non-string fields is unaffected by query collation
            createCompoundIndex(CompoundIndexDefinition.create("age_score_idx", List.of(
                    new CompoundIndexField("age", BsonType.INT32, false),
                    new CompoundIndexField("score", BsonType.INT32, false)
            ), IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ 'age': 25, 'score': 100 }", null, collation("tr"));

            assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        }

        @Test
        void shouldUseCompoundIndexWhenNoQueryCollation() {
            // Behavior: compound index is used normally when query specifies no collation (existing behavior preserved)
            createCompoundIndex(CompoundIndexDefinition.create("name_age_idx", List.of(
                    new CompoundIndexField("name", BsonType.STRING, false),
                    new CompoundIndexField("age", BsonType.INT32, false)
            ), IndexStatus.WAITING, collation("en")));

            PhysicalNode result = planQuery("{ 'name': 'alice', 'age': 25 }");

            assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        }
    }
}
