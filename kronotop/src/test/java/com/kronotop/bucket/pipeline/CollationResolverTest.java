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

package com.kronotop.bucket.pipeline;

import com.google.common.cache.CacheBuilder;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.index.*;
import org.bson.BsonType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class CollationResolverTest {

    private static final UUID TEST_UUID = UUID.fromString("00000000-0000-0000-0000-000000000001");

    private static Collation collation(String locale) {
        return Collation.create(locale, 3, null, null, null, null, null, null, null);
    }

    private static BucketMetadata metadata(SingleFieldIndexRegistry indexes, Collation bucketCollation) {
        return new BucketMetadata(
                TEST_UUID, "default", "test_bucket", 1L, false,
                null, null, null, indexes,
                new CompoundIndexRegistry(null),
                new VectorIndexRegistry(),
                List.of(0), bucketCollation, CacheBuilder.newBuilder().maximumSize(10_000).build()
        );
    }

    private static BucketMetadata metadataWithCompound(CompoundIndexRegistry compoundIndexes, Collation bucketCollation) {
        return new BucketMetadata(
                TEST_UUID, "default", "test_bucket", 1L, false,
                null, null, null, new SingleFieldIndexRegistry(null),
                compoundIndexes,
                new VectorIndexRegistry(),
                List.of(0), bucketCollation, CacheBuilder.newBuilder().maximumSize(10_000).build()
        );
    }

    @Nested
    @DisplayName("Single-field index collation")
    class SingleFieldIndex {

        @Test
        void shouldReturnSingleFieldIndexCollation() {
            // Behavior: returns collation from the single-field index on the selector
            SingleFieldIndexRegistry indexes = new SingleFieldIndexRegistry(null);
            SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create(
                    "name_idx", "name", BsonType.STRING, false, IndexStatus.READY, collation("tr"));
            indexes.register(def, null);

            BucketMetadata meta = metadata(indexes, null);
            Collation result = CollationResolver.resolve(meta, "name", null);

            assertNotNull(result);
            assertEquals("tr", result.locale());
        }

        @Test
        void shouldPreferSingleFieldOverBucket() {
            // Behavior: single-field index collation takes precedence over bucket-level collation
            SingleFieldIndexRegistry indexes = new SingleFieldIndexRegistry(null);
            SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create(
                    "name_idx", "name", BsonType.STRING, false, IndexStatus.READY, collation("tr"));
            indexes.register(def, null);

            BucketMetadata meta = metadata(indexes, collation("en"));
            Collation result = CollationResolver.resolve(meta, "name", null);

            assertEquals("tr", result.locale());
        }

        @Test
        void shouldFallBackToBucketCollation() {
            // Behavior: when single-field index has no collation, bucket collation is used
            SingleFieldIndexRegistry indexes = new SingleFieldIndexRegistry(null);
            SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create(
                    "name_idx", "name", BsonType.STRING, false, IndexStatus.READY);
            indexes.register(def, null);

            BucketMetadata meta = metadata(indexes, collation("en"));
            Collation result = CollationResolver.resolve(meta, "name", null);

            assertNotNull(result);
            assertEquals("en", result.locale());
        }

        @Test
        void shouldReturnBucketWhenNoIndexOnField() {
            // Behavior: when no index exists on the field, bucket collation is returned
            SingleFieldIndexRegistry indexes = new SingleFieldIndexRegistry(null);

            BucketMetadata meta = metadata(indexes, collation("en"));
            Collation result = CollationResolver.resolve(meta, "name", null);

            assertNotNull(result);
            assertEquals("en", result.locale());
        }

        @Test
        void shouldReturnNullWhenNoCollationAnywhere() {
            // Behavior: when no index collation and no bucket collation, returns null
            SingleFieldIndexRegistry indexes = new SingleFieldIndexRegistry(null);

            BucketMetadata meta = metadata(indexes, null);
            Collation result = CollationResolver.resolve(meta, "name", null);

            assertNull(result);
        }

        @Test
        void shouldSkipDroppedIndex() {
            // Behavior: DROPPED indexes are invisible under READ policy, falls back to bucket
            SingleFieldIndexRegistry indexes = new SingleFieldIndexRegistry(null);
            SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create(
                    "name_idx", "name", BsonType.STRING, false, IndexStatus.DROPPED, collation("tr"));
            indexes.register(def, null);

            BucketMetadata meta = metadata(indexes, collation("en"));
            Collation result = CollationResolver.resolve(meta, "name", null);

            assertEquals("en", result.locale());
        }

        @Test
        void shouldSkipBuildingIndexForRead() {
            // Behavior: BUILDING indexes are invisible under READ policy, falls back to bucket
            SingleFieldIndexRegistry indexes = new SingleFieldIndexRegistry(null);
            SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create(
                    "name_idx", "name", BsonType.STRING, false, IndexStatus.BUILDING, collation("tr"));
            indexes.register(def, null);

            BucketMetadata meta = metadata(indexes, collation("en"));
            Collation result = CollationResolver.resolve(meta, "name", null);

            assertEquals("en", result.locale());
        }
    }

    @Nested
    @DisplayName("Query-level collation")
    class QueryCollation {

        @Test
        void shouldReturnQueryCollationWhenSet() {
            // Behavior: query-level collation is returned even when no index or bucket collation exists
            SingleFieldIndexRegistry indexes = new SingleFieldIndexRegistry(null);
            BucketMetadata meta = metadata(indexes, null);

            Collation result = CollationResolver.resolve(meta, "name", collation("fr"));

            assertNotNull(result);
            assertEquals("fr", result.locale());
        }

        @Test
        void shouldPreferQueryOverIndex() {
            // Behavior: query-level collation overrides index-level collation
            SingleFieldIndexRegistry indexes = new SingleFieldIndexRegistry(null);
            SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create(
                    "name_idx", "name", BsonType.STRING, false, IndexStatus.READY, collation("tr"));
            indexes.register(def, null);

            BucketMetadata meta = metadata(indexes, null);
            Collation result = CollationResolver.resolve(meta, "name", collation("fr"));

            assertEquals("fr", result.locale());
        }

        @Test
        void shouldPreferQueryOverBucket() {
            // Behavior: query-level collation overrides bucket-level collation
            SingleFieldIndexRegistry indexes = new SingleFieldIndexRegistry(null);
            BucketMetadata meta = metadata(indexes, collation("en"));

            Collation result = CollationResolver.resolve(meta, "name", collation("fr"));

            assertEquals("fr", result.locale());
        }

        @Test
        void shouldPreferQueryOverBothIndexAndBucket() {
            // Behavior: query-level collation overrides both index-level and bucket-level collation
            SingleFieldIndexRegistry indexes = new SingleFieldIndexRegistry(null);
            SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create(
                    "name_idx", "name", BsonType.STRING, false, IndexStatus.READY, collation("tr"));
            indexes.register(def, null);

            BucketMetadata meta = metadata(indexes, collation("en"));
            Collation result = CollationResolver.resolve(meta, "name", collation("fr"));

            assertEquals("fr", result.locale());
        }

        @Test
        void shouldFallBackToIndexWhenQueryNull() {
            // Behavior: null query collation falls back to index-level collation
            SingleFieldIndexRegistry indexes = new SingleFieldIndexRegistry(null);
            SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create(
                    "name_idx", "name", BsonType.STRING, false, IndexStatus.READY, collation("tr"));
            indexes.register(def, null);

            BucketMetadata meta = metadata(indexes, collation("en"));
            Collation result = CollationResolver.resolve(meta, "name", null);

            assertEquals("tr", result.locale());
        }

        @Test
        void shouldFallBackToBucketWhenQueryAndIndexNull() {
            // Behavior: null query collation and no index collation falls back to bucket-level collation
            SingleFieldIndexRegistry indexes = new SingleFieldIndexRegistry(null);
            SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create(
                    "name_idx", "name", BsonType.STRING, false, IndexStatus.READY);
            indexes.register(def, null);

            BucketMetadata meta = metadata(indexes, collation("en"));
            Collation result = CollationResolver.resolve(meta, "name", null);

            assertEquals("en", result.locale());
        }
    }

    @Nested
    @DisplayName("Compound index collation")
    class CompoundIndex {

        @Test
        void shouldUseCollationFromSingleCompoundIndexWithStringField() {
            // Behavior: a single compound index containing the selector as STRING provides its collation for residual evaluation
            CompoundIndexRegistry compoundIndexes = new CompoundIndexRegistry(null);
            CompoundIndexDefinition def = CompoundIndexDefinition.create(
                    "name_age_idx",
                    List.of(
                            new CompoundIndexField("name", BsonType.STRING, false),
                            new CompoundIndexField("age", BsonType.INT32, false)
                    ),
                    IndexStatus.READY,
                    collation("de")
            );
            compoundIndexes.register(def, null);

            BucketMetadata meta = metadataWithCompound(compoundIndexes, collation("en"));
            Collation result = CollationResolver.resolve(meta, "name", null);

            assertEquals("de", result.locale());
        }

        @Test
        void shouldFallBackToBucketWhenCompoundIndexCollationsConflict() {
            // Behavior: when two compound indexes both contain the selector as STRING
            //           but with different collations, bucket collation is used
            CompoundIndexRegistry compoundIndexes = new CompoundIndexRegistry(null);
            CompoundIndexDefinition def1 = CompoundIndexDefinition.create("idx_name_age",
                    List.of(new CompoundIndexField("name", BsonType.STRING, false),
                            new CompoundIndexField("age", BsonType.INT32, false)),
                    IndexStatus.READY, collation("de"));
            CompoundIndexDefinition def2 = CompoundIndexDefinition.create("idx_name_score",
                    List.of(new CompoundIndexField("name", BsonType.STRING, false),
                            new CompoundIndexField("score", BsonType.INT32, false)),
                    IndexStatus.READY, collation("fr"));
            compoundIndexes.register(def1, null);
            compoundIndexes.register(def2, null);

            BucketMetadata meta = metadataWithCompound(compoundIndexes, collation("en"));
            Collation result = CollationResolver.resolve(meta, "name", null);

            assertEquals("en", result.locale());
        }

        @Test
        void shouldSkipCompoundIndexWhenFieldIsNotString() {
            // Behavior: compound index entries where the selector's bsonType is not STRING
            //           are not considered for collation resolution
            CompoundIndexRegistry compoundIndexes = new CompoundIndexRegistry(null);
            CompoundIndexDefinition def = CompoundIndexDefinition.create("idx_age_score",
                    List.of(new CompoundIndexField("age", BsonType.INT32, false),
                            new CompoundIndexField("score", BsonType.DOUBLE, false)),
                    IndexStatus.READY, collation("de"));
            compoundIndexes.register(def, null);

            BucketMetadata meta = metadataWithCompound(compoundIndexes, collation("en"));
            Collation result = CollationResolver.resolve(meta, "age", null);

            assertEquals("en", result.locale());
        }

        @Test
        void shouldSkipCompoundIndexWithNoCollationAndFallBackToBucket() {
            // Behavior: a compound index containing the selector as STRING but with no collation
            //           does not contribute; bucket collation is used
            CompoundIndexRegistry compoundIndexes = new CompoundIndexRegistry(null);
            CompoundIndexDefinition def = CompoundIndexDefinition.create("idx_name_age",
                    List.of(new CompoundIndexField("name", BsonType.STRING, false),
                            new CompoundIndexField("age", BsonType.INT32, false)),
                    IndexStatus.READY, null);
            compoundIndexes.register(def, null);

            BucketMetadata meta = metadataWithCompound(compoundIndexes, collation("en"));
            Collation result = CollationResolver.resolve(meta, "name", null);

            assertEquals("en", result.locale());
        }

        @Test
        void shouldIgnoreNullCollationCompoundIndexInAgreementCheck() {
            // Behavior: a compound index with null collation is excluded from the agreement check;
            //           the remaining index's collation wins without triggering a conflict
            CompoundIndexRegistry compoundIndexes = new CompoundIndexRegistry(null);
            CompoundIndexDefinition def1 = CompoundIndexDefinition.create("idx_name_age",
                    List.of(new CompoundIndexField("name", BsonType.STRING, false),
                            new CompoundIndexField("age", BsonType.INT32, false)),
                    IndexStatus.READY, collation("de"));
            CompoundIndexDefinition def2 = CompoundIndexDefinition.create("idx_name_score",
                    List.of(new CompoundIndexField("name", BsonType.STRING, false),
                            new CompoundIndexField("score", BsonType.INT32, false)),
                    IndexStatus.READY, null);
            compoundIndexes.register(def1, null);
            compoundIndexes.register(def2, null);

            BucketMetadata meta = metadataWithCompound(compoundIndexes, collation("en"));
            Collation result = CollationResolver.resolve(meta, "name", null);

            assertEquals("de", result.locale());
        }
    }
}
