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

import com.apple.foundationdb.Transaction;
import com.kronotop.TestUtil;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.handlers.protocol.SortDirection;
import com.kronotop.bucket.index.CompoundIndexDefinition;
import com.kronotop.bucket.index.CompoundIndexField;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.planner.physical.PhysicalPlanValidationException;
import org.bson.*;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class CollationIntegrationTest extends BasePipelineTest {

    @Test
    void shouldMatchTurkishDottedIWithTurkishCollation() {
        // Behavior: Turkish collation at PRIMARY strength treats "istanbul" and "\u0130stanbul" as equal,
        // because in Turkish locale i/\u0130 are the same base character pair.

        final String BUCKET_NAME = "test-collation-turkish-dotted-i";

        BucketMetadata metadata = createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"tr\", \"strength\": 1}");

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'istanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '\u0130stanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'ankara'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'izmir'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("\u0130stanbul")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Turkish PRIMARY collation should match both 'istanbul' and '\u0130stanbul'");
            assertEquals(Set.of("istanbul", "\u0130stanbul"), extractNamesFromResults(results));
        }
    }

    private BucketMetadata createBucketWithCollationAndIndex(String bucketName, Collation bucketCollation, String selector, Collation indexCollation) {
        String collationJson = String.format("{\"locale\": \"%s\", \"strength\": %d}", bucketCollation.locale(), bucketCollation.strength());
        createBucketWithCollation(bucketName, collationJson);
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create(
                selector + "_1", selector, BsonType.STRING, false, IndexStatus.WAITING, indexCollation
        );
        createIndexThenWaitForReadiness(TEST_NAMESPACE, bucketName, indexDef);
        return getBucketMetadata(bucketName);
    }

    private BucketMetadata createBucketWithCollationAndCompoundIndex(String bucketName, Collation bucketCollation,
                                                                     CompoundIndexDefinition compoundIndexDef) {
        String collationJson = String.format("{\"locale\": \"%s\", \"strength\": %d}", bucketCollation.locale(), bucketCollation.strength());
        createBucketWithCollation(bucketName, collationJson);
        createIndexThenWaitForReadiness(TEST_NAMESPACE, bucketName, compoundIndexDef);
        return getBucketMetadata(bucketName);
    }

    @Test
    void shouldMatchTurkishDottedIViaIndexScan() {
        // Behavior: With a collated index on "name", an $eq query uses IndexScanNode
        // and matches collation-equivalent strings via sort key bytes.

        final String BUCKET_NAME = "test-collated-index-turkish-eq";

        Collation turkishPrimary = Collation.create("tr", 1, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, turkishPrimary, "name", turkishPrimary);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'istanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '\u0130stanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'ankara'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'izmir'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("\u0130stanbul")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(IndexScanNode.class, planWithParams.plan(),
                "Query should use IndexScanNode when a collated index exists");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Turkish PRIMARY collation via index should match both 'istanbul' and '\u0130stanbul'");
            assertEquals(Set.of("istanbul", "\u0130stanbul"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldRangeScanWithCollatedIndex() {
        // Behavior: Range scan on a collated index uses sort key byte ordering,
        // so $gt/$lt comparisons respect collation rules.

        final String BUCKET_NAME = "test-collated-index-range-scan";

        Collation englishPrimary = Collation.create("en", 1, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, englishPrimary, "name", englishPrimary);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'apple'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Banana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cherry'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Date'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        // With PRIMARY strength, case is ignored. Range $gt:"banana" should pick up "cherry" and "Date"
        // because sort keys at PRIMARY strength: apple < banana = Banana < cherry < date = Date
        BsonDocument query = new BsonDocument("name", new BsonDocument("$gt", new BsonString("banana")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Range scan with PRIMARY collation should return 'cherry' and 'Date' (both > 'banana' in collation order)");
            assertEquals(Set.of("cherry", "Date"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldNotMatchTurkishDottedIWithoutCollation() {
        // Behavior: Without collation, binary comparison treats "istanbul" and "\u0130stanbul" as different
        // strings because their byte representations differ.

        final String BUCKET_NAME = "test-no-collation-turkish-dotted-i";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'istanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '\u0130stanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'ankara'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'izmir'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("\u0130stanbul")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(), "Binary comparison should only match the exact '\u0130stanbul' document");
            assertEquals(Set.of("\u0130stanbul"), extractNamesFromResults(results));
        }
    }

    // --- Collation hierarchy tests ---

    @Test
    void shouldUseIndexCollationOverBucketCollation() {
        // Behavior: Bucket has English collation, index has Turkish collation. Turkish PRIMARY: i\u2261\u0130
        // but i\u2262I (dotless I is a different base). English PRIMARY: i\u2261I\u2261\u0130 (all same).
        // Getting exactly 2 results proves Turkish (index) collation is active, not English (bucket).

        final String BUCKET_NAME = "test-index-collation-over-bucket";

        Collation englishPrimary = Collation.create("en", 1, null, null, null, null, null, null, null);
        Collation turkishPrimary = Collation.create("tr", 1, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, englishPrimary, "name", turkishPrimary);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'idea'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Idea'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '\u0130dea'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("idea")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(IndexScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Turkish PRIMARY on index: i\u2261\u0130 (2 matches). English would give 3, binary would give 1");
            assertEquals(Set.of("idea", "\u0130dea"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldUseBucketCollationInFullScanWhenNoIndex() {
        // Behavior: Bucket has Turkish collation (PRIMARY), no index on the field. Full scan uses
        // bucket collation. Turkish PRIMARY: i\u2261\u0130 but i\u2262I (dotless I is different base).
        // Exactly 2 matches proves Turkish is active (binary=1, English=3).

        final String BUCKET_NAME = "test-bucket-collation-fullscan";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"tr\", \"strength\": 1}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'istanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '\u0130stanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'ISTANBUL'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("istanbul")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Turkish PRIMARY: i\u2261\u0130 (2 matches). Binary would give 1, English PRIMARY would give 3");
            assertEquals(Set.of("istanbul", "\u0130stanbul"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldUseInheritedBucketCollationViaIndex() {
        // Behavior: Index created without explicit collation on a collated bucket inherits bucket
        // collation via BaseIndexMaintainer.resolveCollation(). Turkish PRIMARY treats i\u2261\u0130.

        final String BUCKET_NAME = "test-inherited-bucket-collation";

        Collation turkishPrimary = Collation.create("tr", 1, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, turkishPrimary, "name", null);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'istanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '\u0130stanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'ankara'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("\u0130stanbul")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Inherited Turkish collation should match both 'istanbul' and '\u0130stanbul'");
            assertEquals(Set.of("istanbul", "\u0130stanbul"), extractNamesFromResults(results));
        }
    }

    // --- Range query tests with ICU-sensitive data ---

    @Test
    void shouldRangeScanIncludeAccentedCharactersWithCollatedIndex() {
        // Behavior: French collation at PRIMARY treats \u00c9 as base-e. Binary UTF-8: \u00c9 = 0xC3 0xA9 > 'f' (0x66),
        // so binary range [e,f) misses \u00c9clair. ICU PRIMARY includes it.

        final String BUCKET_NAME = "test-range-accented-french";

        Collation frenchPrimary = Collation.create("fr", 1, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, frenchPrimary, "name", frenchPrimary);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'eclair'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '\u00c9clair'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'fig'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'drag\u00e9e'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument()
                .append("$gte", new BsonString("e"))
                .append("$lt", new BsonString("f")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "French PRIMARY: \u00c9\u2261e, range [e,f) should include both 'eclair' and '\u00c9clair'");
            assertEquals(Set.of("eclair", "\u00c9clair"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldRangeScanIncludeTurkishDottedIWithCollatedIndex() {
        // Behavior: Turkish collation at PRIMARY treats \u0130 as base-i. Binary UTF-8: \u0130 = 0xC4 0xB0 > 'k' (0x6B),
        // so binary range [i,k) misses \u0130skender. Turkish ICU PRIMARY includes it.

        final String BUCKET_NAME = "test-range-turkish-dotted-i";

        Collation turkishPrimary = Collation.create("tr", 1, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, turkishPrimary, "name", turkishPrimary);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'iskender'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '\u0130skender'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'kebap'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'humus'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument()
                .append("$gte", new BsonString("i"))
                .append("$lt", new BsonString("k")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Turkish PRIMARY: \u0130\u2261i, range [i,k) should include both 'iskender' and '\u0130skender'");
            assertEquals(Set.of("iskender", "\u0130skender"), extractNamesFromResults(results));
        }
    }

    // --- Compound index collation tests ---

    @Test
    void shouldMatchWithCollatedCompoundIndexEqQuery() {
        // Behavior: Compound index with Turkish collation matches collation-equivalent strings.
        // Binary would only match exact "\u0130stanbul", ICU Turkish PRIMARY matches both i/\u0130 variants.

        final String BUCKET_NAME = "test-compound-collation-eq";

        Collation turkishPrimary = Collation.create("tr", 1, null, null, null, null, null, null, null);
        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING, turkishPrimary);

        BucketMetadata metadata = createBucketWithCollationAndCompoundIndex(
                BUCKET_NAME, turkishPrimary, compoundIdx);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'istanbul', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '\u0130stanbul', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'ankara', 'age': 30}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("name", new BsonString("\u0130stanbul"))
                .append("age", new BsonInt32(25));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Turkish PRIMARY compound index: both 'istanbul' and '\u0130stanbul' with age=25 should match");
            assertEquals(Set.of("istanbul", "\u0130stanbul"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldRangeScanWithCollatedCompoundIndex() {
        // Behavior: Compound index with French collation handles range queries where accented
        // characters fall within their base letter's range. Binary: \u00c9 (0xC3) > 'f' (0x66),
        // \u00e2 (0xC3 0xA2) \u2260 'a'. ICU PRIMARY: \u00e2\u2261a, \u00c9\u2261e.

        final String BUCKET_NAME = "test-compound-collation-range";

        Collation frenchPrimary = Collation.create("fr", 1, null, null, null, null, null, null, null);
        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_category_name", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("name", BsonType.STRING, false)
        ), IndexStatus.WAITING, frenchPrimary);

        BucketMetadata metadata = createBucketWithCollationAndCompoundIndex(
                BUCKET_NAME, frenchPrimary, compoundIdx);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'category': 'p\u00e2tisserie', 'name': 'eclair'}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'p\u00e2tisserie', 'name': '\u00c9clair'}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'p\u00e2tisserie', 'name': 'financier'}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'boulangerie', 'name': 'baguette'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("category", new BsonString("p\u00e2tisserie"))
                .append("name", new BsonDocument()
                        .append("$gte", new BsonString("e"))
                        .append("$lt", new BsonString("f")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "French PRIMARY compound index: \u00e2\u2261a matches 'p\u00e2tisserie', \u00c9\u2261e puts '\u00c9clair' in [e,f)");
            assertEquals(Set.of("eclair", "\u00c9clair"), extractNamesFromResults(results));
        }
    }

    // --- SECONDARY (strength=2) tests ---

    @Test
    void shouldMatchCaseInsensitivelyButDistinguishAccentsWithSecondaryFullScan() {
        // Behavior: French SECONDARY ignores case but distinguishes accents. $eq "cafe" matches
        // "cafe" and "Cafe" (case ignored) but not "caf\u00e9" (accent differs).

        final String BUCKET_NAME = "test-secondary-fullscan-french";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"fr\", \"strength\": 2}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'caf\u00e9'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("cafe")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "French SECONDARY: case-insensitive (cafe=Cafe) but accent-sensitive (caf\u00e9\u2260cafe). PRIMARY would give 3, TERTIARY would give 1");
            assertEquals(Set.of("cafe", "Cafe"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchCaseInsensitivelyButDistinguishAccentsWithSecondaryIndexScan() {
        // Behavior: French SECONDARY on a collated index ignores case but distinguishes accents
        // via sort key bytes. IndexScanNode correctly uses SECONDARY collation keys.

        final String BUCKET_NAME = "test-secondary-index-scan-french";

        Collation frenchSecondary = Collation.create("fr", 2, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, frenchSecondary, "name", frenchSecondary);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'caf\u00e9'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'baguette'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("cafe")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(IndexScanNode.class, planWithParams.plan(),
                "Query should use IndexScanNode when a collated index exists");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "French SECONDARY via index: case-insensitive but accent-sensitive. PRIMARY would give 3, TERTIARY would give 1");
            assertEquals(Set.of("cafe", "Cafe"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldRangeScanWithSecondaryStrength() {
        // Behavior: French SECONDARY $gt "cafe". "cafe"/"Cafe" share the same sort key (case ignored),
        // so $gt excludes both. "caf\u00e9" has a different sort key (accent matters) and is strictly
        // greater, so it's included along with "restaurant".

        final String BUCKET_NAME = "test-secondary-range-scan-french";

        Collation frenchSecondary = Collation.create("fr", 2, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, frenchSecondary, "name", frenchSecondary);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'caf\u00e9'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'restaurant'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gt", new BsonString("cafe")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "SECONDARY $gt 'cafe': cafe/Cafe share sort key (excluded), caf\u00e9 is strictly greater. PRIMARY would give 1, TERTIARY would give 3");
            assertEquals(Set.of("caf\u00e9", "restaurant"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchCaseInsensitivelyWithSecondaryCompoundIndex() {
        // Behavior: Compound index with French SECONDARY matches case-insensitively but
        // distinguishes accents across both fields of the compound key.

        final String BUCKET_NAME = "test-secondary-compound-french";

        Collation frenchSecondary = Collation.create("fr", 2, null, null, null, null, null, null, null);
        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_city", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("city", BsonType.STRING, false)
        ), IndexStatus.WAITING, frenchSecondary);

        BucketMetadata metadata = createBucketWithCollationAndCompoundIndex(
                BUCKET_NAME, frenchSecondary, compoundIdx);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe', 'city': 'paris'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe', 'city': 'paris'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'caf\u00e9', 'city': 'paris'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe', 'city': 'lyon'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("name", new BsonString("cafe"))
                .append("city", new BsonString("Paris"));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "SECONDARY compound: cafe/Cafe match (case-insensitive), caf\u00e9 excluded (accent-sensitive), Paris=paris (case-insensitive)");
            assertEquals(Set.of("cafe", "Cafe"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldInheritBucketSecondaryCollationViaIndex() {
        // Behavior: Index created without explicit collation on a SECONDARY-collated bucket inherits
        // the bucket's SECONDARY strength. Case-insensitive but accent-sensitive matching.

        final String BUCKET_NAME = "test-inherited-secondary-collation";

        Collation frenchSecondary = Collation.create("fr", 2, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, frenchSecondary, "name", null);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'caf\u00e9'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'baguette'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("cafe")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Inherited SECONDARY: case-insensitive (cafe=Cafe), accent-sensitive (caf\u00e9\u2260cafe). Binary would give 1, PRIMARY would give 3");
            assertEquals(Set.of("cafe", "Cafe"), extractNamesFromResults(results));
        }
    }

    // --- TERTIARY (strength=3) tests ---

    @Test
    void shouldMatchExactlyWithTertiaryFullScan() {
        // Behavior: French TERTIARY distinguishes both case and accents. Only the exact string matches.

        final String BUCKET_NAME = "test-tertiary-fullscan-french";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"fr\", \"strength\": 3}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'caf\u00e9'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("cafe")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(),
                    "French TERTIARY: both case and accent sensitive. PRIMARY would give 3, SECONDARY would give 2");
            assertEquals(Set.of("cafe"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchExactlyWithTertiaryIndexScan() {
        // Behavior: French TERTIARY on a collated index matches only the exact string.
        // Sort key bytes encode both case and accent differences.

        final String BUCKET_NAME = "test-tertiary-index-scan-french";

        Collation frenchTertiary = Collation.create("fr", 3, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, frenchTertiary, "name", frenchTertiary);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'caf\u00e9'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'baguette'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("cafe")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(IndexScanNode.class, planWithParams.plan(),
                "Query should use IndexScanNode when a collated index exists");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(),
                    "French TERTIARY via index: both case and accent sensitive. PRIMARY would give 3, SECONDARY would give 2");
            assertEquals(Set.of("cafe"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldRangeScanWithTertiaryStrength() {
        // Behavior: French TERTIARY $gt "cafe". Every case/accent variant has a distinct sort key,
        // so "Cafe" and "caf\u00e9" are both strictly greater than "cafe".

        final String BUCKET_NAME = "test-tertiary-range-scan-french";

        Collation frenchTertiary = Collation.create("fr", 3, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, frenchTertiary, "name", frenchTertiary);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'caf\u00e9'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'restaurant'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gt", new BsonString("cafe")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(3, results.size(),
                    "TERTIARY $gt 'cafe': all variants have distinct sort keys, 3 are strictly greater. PRIMARY would give 1, SECONDARY would give 2");
            assertEquals(Set.of("Cafe", "caf\u00e9", "restaurant"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchExactlyWithTertiaryCompoundIndex() {
        // Behavior: Compound index with English TERTIARY requires exact case and accent match
        // across all fields.

        final String BUCKET_NAME = "test-tertiary-compound-english";

        Collation englishTertiary = Collation.create("en", 3, null, null, null, null, null, null, null);
        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_city", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("city", BsonType.STRING, false)
        ), IndexStatus.WAITING, englishTertiary);

        BucketMetadata metadata = createBucketWithCollationAndCompoundIndex(
                BUCKET_NAME, englishTertiary, compoundIdx);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe', 'city': 'paris'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe', 'city': 'paris'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe', 'city': 'Paris'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe', 'city': 'lyon'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("name", new BsonString("cafe"))
                .append("city", new BsonString("paris"));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(),
                    "TERTIARY compound: exact match on both fields. PRIMARY would give 3, SECONDARY would give 2");
            assertEquals(Set.of("cafe"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldDefaultToTertiaryWhenStrengthOmitted() {
        // Behavior: When collation JSON omits "strength", the default is TERTIARY (3). Only the
        // exact string matches, proving the default via exact-match-only behavior.

        final String BUCKET_NAME = "test-default-tertiary-french";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"fr\"}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'caf\u00e9'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("cafe")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(),
                    "Default strength should be TERTIARY: exact match only. If PRIMARY, would give 3; if SECONDARY, would give 2");
            assertEquals(Set.of("cafe"), extractNamesFromResults(results));
        }
    }

    // --- $ne (not-equal) with collation tests ---

    @Test
    void shouldExcludeCollationEquivalentStringsWithNeAndPrimaryFullScan() {
        // Behavior: Turkish PRIMARY $ne excludes all collation-equivalent strings, not just
        // the binary-equal one. i≡İ in Turkish, so $ne "İstanbul" excludes both variants.

        final String BUCKET_NAME = "test-ne-primary-turkish";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"tr\", \"strength\": 1}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'istanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'İstanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'ankara'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'izmir'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$ne", new BsonString("İstanbul")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Turkish PRIMARY $ne: both 'istanbul' and 'İstanbul' excluded (collation-equivalent). Binary would exclude only 1");
            assertEquals(Set.of("ankara", "izmir"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldExcludeCollationEquivalentStringsWithNeAndSecondaryFullScan() {
        // Behavior: French SECONDARY $ne excludes case-insensitive matches but keeps accent
        // variants. $ne "cafe" excludes cafe/Cafe but keeps café (accent differs).

        final String BUCKET_NAME = "test-ne-secondary-french";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"fr\", \"strength\": 2}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'café'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'baguette'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$ne", new BsonString("cafe")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "French SECONDARY $ne: cafe/Cafe excluded (case-insensitive), café kept (accent-sensitive). PRIMARY would exclude 3, TERTIARY would exclude 1");
            assertEquals(Set.of("café", "baguette"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldExcludeOnlyExactMatchWithNeAndTertiaryFullScan() {
        // Behavior: French TERTIARY $ne distinguishes both case and accents. Only the exact
        // string "cafe" is excluded; "Cafe" and "café" are kept.

        final String BUCKET_NAME = "test-ne-tertiary-french";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"fr\", \"strength\": 3}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'café'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$ne", new BsonString("cafe")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "French TERTIARY $ne: only exact 'cafe' excluded. PRIMARY would exclude 3, SECONDARY would exclude 2");
            assertEquals(Set.of("Cafe", "café"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldExcludeCollationEquivalentStringsWithNeViaIndexScan() {
        // Behavior: With a collated index on "name", an NE query uses IndexScanNode and
        // excludes all collation-equivalent strings. Turkish PRIMARY: i≡İ, so $ne "İstanbul"
        // excludes both 'istanbul' and 'İstanbul'.

        final String BUCKET_NAME = "test-ne-collated-index-turkish";

        Collation turkishPrimary = Collation.create("tr", 1, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, turkishPrimary, "name", turkishPrimary);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'istanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'İstanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'ankara'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'izmir'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$ne", new BsonString("İstanbul")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(IndexScanNode.class, planWithParams.plan(),
                "Query should use IndexScanNode when a collated index exists");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Turkish PRIMARY $ne via index: both 'istanbul' and 'İstanbul' excluded (collation-equivalent). Binary would exclude only 1");
            assertEquals(Set.of("ankara", "izmir"), extractNamesFromResults(results));
        }
    }

    // --- $in/$nin with collation tests ---

    @Test
    void shouldMatchCollationEquivalentStringsWithInAndPrimaryFullScan() {
        // Behavior: Turkish PRIMARY $in matches all collation-equivalent variants of each list
        // element. i≡İ in Turkish, so $in ["İstanbul"] matches both variants.

        final String BUCKET_NAME = "test-in-primary-turkish";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"tr\", \"strength\": 1}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'istanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'İstanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'ankara'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'izmir'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$in",
                new BsonArray(List.of(new BsonString("İstanbul")))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Turkish PRIMARY $in: both 'istanbul' and 'İstanbul' match (collation-equivalent). Binary would match only 1");
            assertEquals(Set.of("istanbul", "İstanbul"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchCaseInsensitivelyWithInAndSecondaryFullScan() {
        // Behavior: French SECONDARY $in matches case-insensitively but distinguishes accents.
        // $in ["cafe"] matches "cafe" and "Cafe" (case ignored) but not "café" (accent differs).

        final String BUCKET_NAME = "test-in-secondary-french";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"fr\", \"strength\": 2}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'café'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'baguette'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$in",
                new BsonArray(List.of(new BsonString("cafe")))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "French SECONDARY $in: case-insensitive (cafe=Cafe) but accent-sensitive (café≠cafe). PRIMARY would give 3, TERTIARY would give 1");
            assertEquals(Set.of("cafe", "Cafe"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchOnlyExactWithInAndTertiaryFullScan() {
        // Behavior: French TERTIARY $in distinguishes both case and accents. Only the exact
        // string "cafe" matches.

        final String BUCKET_NAME = "test-in-tertiary-french";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"fr\", \"strength\": 3}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'café'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$in",
                new BsonArray(List.of(new BsonString("cafe")))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(),
                    "French TERTIARY $in: both case and accent sensitive. PRIMARY would give 3, SECONDARY would give 2");
            assertEquals(Set.of("cafe"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldExcludeCollationEquivalentStringsWithNinAndPrimaryFullScan() {
        // Behavior: Turkish PRIMARY $nin excludes all collation-equivalent strings, not just
        // the binary-equal one. i≡İ in Turkish, so $nin ["İstanbul"] excludes both variants.

        final String BUCKET_NAME = "test-nin-primary-turkish";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"tr\", \"strength\": 1}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'istanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'İstanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'ankara'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'izmir'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$nin",
                new BsonArray(List.of(new BsonString("İstanbul")))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Turkish PRIMARY $nin: both 'istanbul' and 'İstanbul' excluded (collation-equivalent). Binary would exclude only 1");
            assertEquals(Set.of("ankara", "izmir"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldExcludeCaseInsensitivelyWithNinAndSecondaryFullScan() {
        // Behavior: French SECONDARY $nin excludes case-insensitive matches but keeps accent
        // variants. $nin ["cafe"] excludes cafe/Cafe but keeps café (accent differs).

        final String BUCKET_NAME = "test-nin-secondary-french";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"fr\", \"strength\": 2}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'café'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'baguette'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$nin",
                new BsonArray(List.of(new BsonString("cafe")))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "French SECONDARY $nin: cafe/Cafe excluded (case-insensitive), café kept (accent-sensitive). PRIMARY would exclude 3, TERTIARY would exclude 1");
            assertEquals(Set.of("café", "baguette"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldExcludeOnlyExactMatchWithNinAndTertiaryFullScan() {
        // Behavior: French TERTIARY $nin distinguishes both case and accents. Only the exact
        // string "cafe" is excluded; "Cafe" and "café" are kept.

        final String BUCKET_NAME = "test-nin-tertiary-french";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"fr\", \"strength\": 3}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Cafe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'café'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$nin",
                new BsonArray(List.of(new BsonString("cafe")))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "French TERTIARY $nin: only exact 'cafe' excluded. PRIMARY would exclude 3, SECONDARY would exclude 2");
            assertEquals(Set.of("Cafe", "café"), extractNamesFromResults(results));
        }
    }

    // --- SORTBY with collation tests ---

    @Test
    void shouldSortByAscOnCollatedSingleFieldIndexFrenchPrimary() {
        // Behavior: French PRIMARY collation sorts accented/case variants next to their base letter.
        // Binary would put 'Eclair' (0x45) before all lowercase; ICU groups e-variants between 'd' and 'f'.

        final String BUCKET_NAME = "test-collation-sortby-french-primary-asc";

        Collation frenchPrimary = Collation.create("fr", 1, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, frenchPrimary, "name", frenchPrimary);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'fig'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'eclair'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'banana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'date'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Eclair'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gte", new BsonString("")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder()
                .sortByField("name")
                .sortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"banana\"}",
                "{\"name\": \"date\"}",
                "{\"name\": \"eclair\"}",
                "{\"name\": \"Eclair\"}",
                "{\"name\": \"fig\"}"
        );
        assertEquals(expectedResult, actualResult,
                "French PRIMARY ASC: accented/case variants of 'e' should sort between 'd' and 'f', not at binary positions");
    }

    @Test
    void shouldSortByDescOnCollatedSingleFieldIndexFrenchPrimary() {
        // Behavior: Same as ASC test but reversed. Proves DESC scan direction works with collated index.

        final String BUCKET_NAME = "test-collation-sortby-french-primary-desc";

        Collation frenchPrimary = Collation.create("fr", 1, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, frenchPrimary, "name", frenchPrimary);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'fig'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'eclair'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'banana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'date'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Eclair'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gte", new BsonString("")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder()
                .sortByField("name")
                .sortDirection(SortDirection.DESC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"fig\"}",
                "{\"name\": \"Eclair\"}",
                "{\"name\": \"eclair\"}",
                "{\"name\": \"date\"}",
                "{\"name\": \"banana\"}"
        );
        assertEquals(expectedResult, actualResult,
                "French PRIMARY DESC: reverse of ASC collation order");
    }

    @Test
    void shouldSortByAscOnCollatedCompoundIndexFrenchPrimary() {
        // Behavior: SORTBY on the second field of a collated compound index respects French PRIMARY
        // collation order. Case variants group together between their neighboring base letters.

        final String BUCKET_NAME = "test-collation-sortby-compound-french-primary-asc";

        Collation frenchPrimary = Collation.create("fr", 1, null, null, null, null, null, null, null);
        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_cat_name", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("name", BsonType.STRING, false)
        ), IndexStatus.WAITING, frenchPrimary);

        BucketMetadata metadata = createBucketWithCollationAndCompoundIndex(
                BUCKET_NAME, frenchPrimary, compoundIdx);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'category': 'patisserie', 'name': 'financier'}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'patisserie', 'name': 'eclair'}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'patisserie', 'name': 'baguette'}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'patisserie', 'name': 'Eclair'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'category': 'patisserie'}", "name");
        assertInstanceOf(CompoundIndexScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder()
                .sortByField("name")
                .sortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"category\": \"patisserie\", \"name\": \"baguette\"}",
                "{\"category\": \"patisserie\", \"name\": \"eclair\"}",
                "{\"category\": \"patisserie\", \"name\": \"Eclair\"}",
                "{\"category\": \"patisserie\", \"name\": \"financier\"}"
        );
        assertEquals(expectedResult, actualResult,
                "French PRIMARY ASC via compound index: e-variants group between 'b' and 'f'");
    }

    @Test
    void shouldSortByDescOnCollatedCompoundIndexFrenchPrimary() {
        // Behavior: Same as ASC compound test but reversed. Proves DESC scan direction works
        // with collated compound index.

        final String BUCKET_NAME = "test-collation-sortby-compound-french-primary-desc";

        Collation frenchPrimary = Collation.create("fr", 1, null, null, null, null, null, null, null);
        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_cat_name", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("name", BsonType.STRING, false)
        ), IndexStatus.WAITING, frenchPrimary);

        BucketMetadata metadata = createBucketWithCollationAndCompoundIndex(
                BUCKET_NAME, frenchPrimary, compoundIdx);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'category': 'patisserie', 'name': 'financier'}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'patisserie', 'name': 'eclair'}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'patisserie', 'name': 'baguette'}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'patisserie', 'name': 'Eclair'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'category': 'patisserie'}", "name");
        assertInstanceOf(CompoundIndexScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder()
                .sortByField("name")
                .sortDirection(SortDirection.DESC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"category\": \"patisserie\", \"name\": \"financier\"}",
                "{\"category\": \"patisserie\", \"name\": \"Eclair\"}",
                "{\"category\": \"patisserie\", \"name\": \"eclair\"}",
                "{\"category\": \"patisserie\", \"name\": \"baguette\"}"
        );
        assertEquals(expectedResult, actualResult,
                "French PRIMARY DESC via compound index: reverse of ASC collation order");
    }

    @Test
    void shouldSortByAscWithTurkishPrimaryCollation() {
        // Behavior: Turkish PRIMARY groups \u0130 (capital dotted I) with 'i' base.
        // Binary UTF-8: \u0130 = 0xC4 0xB0 > 'z' (0x7A), so binary would sort \u0130stanbul after zebra.
        // ICU Turkish PRIMARY places it next to istanbul.

        final String BUCKET_NAME = "test-collation-sortby-turkish-primary-asc";

        Collation turkishPrimary = Collation.create("tr", 1, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, turkishPrimary, "name", turkishPrimary);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'zebra'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'istanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '\u0130stanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'ankara'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gte", new BsonString("")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder()
                .sortByField("name")
                .sortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"ankara\"}",
                "{\"name\": \"istanbul\"}",
                "{\"name\": \"\u0130stanbul\"}",
                "{\"name\": \"zebra\"}"
        );
        assertEquals(expectedResult, actualResult,
                "Turkish PRIMARY ASC: \u0130 (capital dotted I) should sort with 'i' base, not after 'z' as binary would");
    }

    @Test
    void shouldSortByAscWithSpanishPrimaryCollationNTilde() {
        // Behavior: Spanish PRIMARY sorts \u00f1 between 'n' and 'o'.
        // Binary UTF-8: \u00f1 = 0xC3 0xB1 > 'z' (0x7A), so binary would sort \u00f1-words after 'z'.
        // ICU Spanish PRIMARY places \u00f1 as a distinct letter between 'n' and 'o'.

        final String BUCKET_NAME = "test-collation-sortby-spanish-primary-asc";

        Collation spanishPrimary = Collation.create("es", 1, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, spanishPrimary, "name", spanishPrimary);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'ocho'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'nada'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '\u00f1o\u00f1o'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'mucho'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '\u00f1and\u00fa'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gte", new BsonString("")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder()
                .sortByField("name")
                .sortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"mucho\"}",
                "{\"name\": \"nada\"}",
                "{\"name\": \"\u00f1and\u00fa\"}",
                "{\"name\": \"\u00f1o\u00f1o\"}",
                "{\"name\": \"ocho\"}"
        );
        assertEquals(expectedResult, actualResult,
                "Spanish PRIMARY ASC: \u00f1 should sort between 'n' and 'o', not after 'z' as binary would");
    }

    @Test
    void shouldMatchWithShiftedAlternateAndMaxVariableSpace() {
        // Behavior: With alternate=shifted and maxVariable=space, only spaces are ignorable.
        // Punctuation remains significant, so "black-bird" does NOT match "blackbird".

        final String BUCKET_NAME = "test-collation-shifted-max-variable-space";

        BucketMetadata metadata = createBucketWithCollation(BUCKET_NAME,
                "{\"locale\": \"en\", \"strength\": 3, \"alternate\": \"shifted\", \"max_variable\": \"space\"}");

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'blackbird'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'black bird'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'black-bird'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'bluejay'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("blackbird")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "shifted+maxVariable=space should treat spaces as ignorable but not punctuation");
            assertEquals(Set.of("blackbird", "black bird"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchWithShiftedAlternateAndMaxVariablePunct() {
        // Behavior: With alternate=shifted and maxVariable=punct, both spaces and punctuation
        // are ignorable, so "black bird", "black-bird", and "blackbird" all match an $eq query.

        final String BUCKET_NAME = "test-collation-shifted-max-variable-punct";

        BucketMetadata metadata = createBucketWithCollation(BUCKET_NAME,
                "{\"locale\": \"en\", \"strength\": 3, \"alternate\": \"shifted\", \"max_variable\": \"punct\"}");

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'blackbird'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'black bird'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'black-bird'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'bluejay'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("blackbird")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(3, results.size(),
                    "shifted+maxVariable=punct should treat both spaces and punctuation as ignorable");
            assertEquals(Set.of("blackbird", "black bird", "black-bird"), extractNamesFromResults(results));
        }
    }

    // --- Numeric ordering (numericOrdering=true) tests ---

    @Test
    void shouldMatchExactWithEqAndNumericOrderingFullScan() {
        // Behavior: numericOrdering affects sort/range comparisons, not equality. $eq "10" matches only "10".

        final String BUCKET_NAME = "test-numeric-ordering-eq-fullscan";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"en\", \"strength\": 3, \"numeric_ordering\": true}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': '1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '20'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("10")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size(),
                    "Numeric ordering does not change $eq semantics: only exact '10' matches, not '1' or '2' or '20'");
            assertEquals(Set.of("10"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldRangeQueryGtWithNumericOrderingFullScan() {
        // Behavior: numericOrdering=true causes $gt to compare contiguous digit substrings numerically.
        // '10' > '2' and '20' > '2' numerically, whereas lexicographically '10' < '2'.

        final String BUCKET_NAME = "test-numeric-ordering-gt-fullscan";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"en\", \"strength\": 3, \"numeric_ordering\": true}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': '1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '20'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gt", new BsonString("2")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Numeric ordering $gt '2': 10 and 20 are numerically greater than 2. Without numericOrdering, only '20' would match (lexicographic: '10' < '2')");
            assertEquals(Set.of("10", "20"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldRangeQueryLtWithNumericOrderingFullScan() {
        // Behavior: numericOrdering=true causes $lt to compare digit substrings as numbers. 1 < 10 and 2 < 10 numerically.

        final String BUCKET_NAME = "test-numeric-ordering-lt-fullscan";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"en\", \"strength\": 3, \"numeric_ordering\": true}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': '1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '20'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$lt", new BsonString("10")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Numeric ordering $lt '10': 1 and 2 are numerically less than 10");
            assertEquals(Set.of("1", "2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldRangeQueryWithNumericOrderingViaIndexScan() {
        // Behavior: A collated index with numericOrdering=true generates sort keys where digit sequences
        // are compared by numeric value. The index scan correctly ranges over these sort keys.

        final String BUCKET_NAME = "test-numeric-ordering-index-scan";

        Collation numericCollation = Collation.create("en", 3, null, null, true, null, null, null, null);
        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"en\", \"strength\": 3, \"numeric_ordering\": true}");
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create(
                "name_1", "name", BsonType.STRING, false, IndexStatus.WAITING, numericCollation);
        createIndexThenWaitForReadiness(TEST_NAMESPACE, BUCKET_NAME, indexDef);
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': '1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '20'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gt", new BsonString("2")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(IndexScanNode.class, planWithParams.plan(),
                "Query should use IndexScanNode when a collated index with numericOrdering exists");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Numeric ordering via index: $gt '2' returns 10 and 20 (numerically greater). Without numericOrdering, sort keys would place '10' before '2'");
            assertEquals(Set.of("10", "20"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldSortByAscWithNumericOrdering() {
        // Behavior: numericOrdering=true on a collated index causes SORTBY ASC to order digit-only strings
        // numerically: 1 < 2 < 10 < 20, not lexicographic 1 < 10 < 2 < 20.

        final String BUCKET_NAME = "test-numeric-ordering-sortby-asc";

        Collation numericCollation = Collation.create("en", 3, null, null, true, null, null, null, null);
        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"en\", \"strength\": 3, \"numeric_ordering\": true}");
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create(
                "name_1", "name", BsonType.STRING, false, IndexStatus.WAITING, numericCollation);
        createIndexThenWaitForReadiness(TEST_NAMESPACE, BUCKET_NAME, indexDef);
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': '1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '20'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gte", new BsonString("")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder()
                .sortByField("name")
                .sortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"1\"}",
                "{\"name\": \"2\"}",
                "{\"name\": \"10\"}",
                "{\"name\": \"20\"}"
        );
        assertEquals(expectedResult, actualResult,
                "Numeric ordering ASC: 1, 2, 10, 20 (numeric order). Without numericOrdering, would be 1, 10, 2, 20 (lexicographic)");
    }

    @Test
    void shouldSortByDescWithNumericOrdering() {
        // Behavior: numericOrdering=true with SORTBY DESC produces reverse numeric order: 20 > 10 > 2 > 1.

        final String BUCKET_NAME = "test-numeric-ordering-sortby-desc";

        Collation numericCollation = Collation.create("en", 3, null, null, true, null, null, null, null);
        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"en\", \"strength\": 3, \"numeric_ordering\": true}");
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create(
                "name_1", "name", BsonType.STRING, false, IndexStatus.WAITING, numericCollation);
        createIndexThenWaitForReadiness(TEST_NAMESPACE, BUCKET_NAME, indexDef);
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': '1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '20'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gte", new BsonString("")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder()
                .sortByField("name")
                .sortDirection(SortDirection.DESC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"20\"}",
                "{\"name\": \"10\"}",
                "{\"name\": \"2\"}",
                "{\"name\": \"1\"}"
        );
        assertEquals(expectedResult, actualResult,
                "Numeric ordering DESC: 20, 10, 2, 1 (reverse numeric order). Without numericOrdering, would be 20, 2, 10, 1 (reverse lexicographic)");
    }

    @Test
    void shouldSortByAscWithEmbeddedNumbersAndNumericOrdering() {
        // Behavior: ICU numeric collation compares contiguous digit substrings as numbers.
        // 'item' prefix matches literally, then 1 < 2 < 10 < 20 numerically.

        final String BUCKET_NAME = "test-numeric-ordering-embedded-numbers-sortby-asc";

        Collation numericCollation = Collation.create("en", 3, null, null, true, null, null, null, null);
        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"en\", \"strength\": 3, \"numeric_ordering\": true}");
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create(
                "name_1", "name", BsonType.STRING, false, IndexStatus.WAITING, numericCollation);
        createIndexThenWaitForReadiness(TEST_NAMESPACE, BUCKET_NAME, indexDef);
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'item1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'item2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'item10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'item20'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gte", new BsonString("")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder()
                .sortByField("name")
                .sortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"item1\"}",
                "{\"name\": \"item2\"}",
                "{\"name\": \"item10\"}",
                "{\"name\": \"item20\"}"
        );
        assertEquals(expectedResult, actualResult,
                "Numeric ordering with embedded numbers: item1, item2, item10, item20. Without numericOrdering, would be item1, item10, item2, item20 (lexicographic)");
    }

    @Test
    void shouldNotOrderNegativeNumbersNumericallyWithNumericOrdering() {
        // Behavior: ICU numericOrdering only handles contiguous non-negative digit substrings.
        // The '-' character is not a digit; it acts as a separator. After '-', the digits '2' and '10'
        // are compared numerically (2 < 10), so '-2' < '-10'.

        final String BUCKET_NAME = "test-numeric-ordering-negative-numbers-sortby-asc";

        Collation numericCollation = Collation.create("en", 3, null, null, true, null, null, null, null);
        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"en\", \"strength\": 3, \"numeric_ordering\": true}");
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create(
                "name_1", "name", BsonType.STRING, false, IndexStatus.WAITING, numericCollation);
        createIndexThenWaitForReadiness(TEST_NAMESPACE, BUCKET_NAME, indexDef);
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': '-2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '-10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '10'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gte", new BsonString("")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder()
                .sortByField("name")
                .sortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"-2\"}",
                "{\"name\": \"-10\"}",
                "{\"name\": \"1\"}",
                "{\"name\": \"10\"}"
        );
        assertEquals(expectedResult, actualResult,
                "ICU numeric ordering treats '-' as a literal character, not a negative sign. '-2' and '-10' sort before positive numbers, and '-2' < '-10' because after the '-', 2 < 10 numerically");
    }

    @Test
    void shouldTreatDecimalPointAsSeparatorWithNumericOrdering() {
        // Behavior: numericOrdering does not support decimal values. The '.' acts as a literal separator.
        // Digit groups on each side are compared independently as integers: 2=2, .=., then 1 < 2 < 10 < 20.

        final String BUCKET_NAME = "test-numeric-ordering-decimal-point-sortby-asc";

        Collation numericCollation = Collation.create("en", 3, null, null, true, null, null, null, null);
        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"en\", \"strength\": 3, \"numeric_ordering\": true}");
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create(
                "name_1", "name", BsonType.STRING, false, IndexStatus.WAITING, numericCollation);
        createIndexThenWaitForReadiness(TEST_NAMESPACE, BUCKET_NAME, indexDef);
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': '2.1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '2.2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '2.10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '2.20'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gte", new BsonString("")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder()
                .sortByField("name")
                .sortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"2.1\"}",
                "{\"name\": \"2.2\"}",
                "{\"name\": \"2.10\"}",
                "{\"name\": \"2.20\"}"
        );
        assertEquals(expectedResult, actualResult,
                "ICU numeric ordering treats '.' as a literal separator, not a decimal point. After the '.', digits are compared numerically: 1 < 2 < 10 < 20. Without numericOrdering, order would be 2.1, 2.10, 2.2, 2.20 (lexicographic)");
    }

    @Test
    void shouldSortLexicographicallyWithoutNumericOrdering() {
        // Behavior: Without numericOrdering, digit strings are compared character-by-character:
        // '1' < '10' < '2' < '20' lexicographically. This is the baseline for contrast with numeric ordering tests.

        final String BUCKET_NAME = "test-no-numeric-ordering-sortby-asc";

        Collation noNumericCollation = Collation.create("en", 3, null, null, null, null, null, null, null);
        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"en\", \"strength\": 3}");
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create(
                "name_1", "name", BsonType.STRING, false, IndexStatus.WAITING, noNumericCollation);
        createIndexThenWaitForReadiness(TEST_NAMESPACE, BUCKET_NAME, indexDef);
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': '1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '20'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gte", new BsonString("")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder()
                .sortByField("name")
                .sortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"1\"}",
                "{\"name\": \"10\"}",
                "{\"name\": \"2\"}",
                "{\"name\": \"20\"}"
        );
        assertEquals(expectedResult, actualResult,
                "Without numericOrdering, sort is lexicographic: 1, 10, 2, 20. With numericOrdering, would be 1, 2, 10, 20");
    }

    @Test
    void shouldApplyNumericOrderingViaQueryLevelCollationOverride() {
        // Behavior: Query-level collation with numericOrdering=true overrides the bucket's collation
        // (which has numericOrdering=false). The residual predicate evaluates $gt using numeric
        // comparison of digit substrings.

        final String BUCKET_NAME = "test-numeric-ordering-query-level-override";

        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"en\", \"strength\": 3}");
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': '1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '20'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gt", new BsonString("2")));

        Collation numericCollation = Collation.create("en", 3, null, null, true, null, null, null, null);
        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query), null, numericCollation);
        assertInstanceOf(FullScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Query-level numericOrdering=true overrides bucket (no numericOrdering). $gt '2' returns 10 and 20 numerically. Without the override, only '20' would match lexicographically");
            assertEquals(Set.of("10", "20"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldTreatPlusPrefixAsSeparatorWithNumericOrdering() {
        // Behavior: ICU numericOrdering treats '+' as a literal separator, not a positive sign.
        // After '+', digit substrings are compared numerically: +2 < +10. The '+' character
        // sorts before digits, so +2 and +10 sort before bare 1 and 10.

        final String BUCKET_NAME = "test-numeric-ordering-plus-prefix-sortby-asc";

        Collation numericCollation = Collation.create("en", 3, null, null, true, null, null, null, null);
        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"en\", \"strength\": 3, \"numeric_ordering\": true}");
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create(
                "name_1", "name", BsonType.STRING, false, IndexStatus.WAITING, numericCollation);
        createIndexThenWaitForReadiness(TEST_NAMESPACE, BUCKET_NAME, indexDef);
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': '+2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '+10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '10'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gte", new BsonString("")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder()
                .sortByField("name")
                .sortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"+2\"}",
                "{\"name\": \"+10\"}",
                "{\"name\": \"1\"}",
                "{\"name\": \"10\"}"
        );
        assertEquals(expectedResult, actualResult,
                "ICU numeric ordering treats '+' as a literal separator, not a positive sign. '+2' < '+10' (numerically after separator), and both sort before bare digits because '+' precedes digit characters");
    }

    @Test
    void shouldTreatExponentNotationAsLiteralWithNumericOrdering() {
        // Behavior: ICU numericOrdering does not interpret exponent notation. The letter 'e' acts as a
        // literal separator. Digit groups on each side are compared independently as integers:
        // 1e5 vs 2e3 → first group 1 < 2, so 1e5 < 2e3.

        final String BUCKET_NAME = "test-numeric-ordering-exponent-notation-sortby-asc";

        Collation numericCollation = Collation.create("en", 3, null, null, true, null, null, null, null);
        createBucketWithCollation(BUCKET_NAME, "{\"locale\": \"en\", \"strength\": 3, \"numeric_ordering\": true}");
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create(
                "name_1", "name", BsonType.STRING, false, IndexStatus.WAITING, numericCollation);
        createIndexThenWaitForReadiness(TEST_NAMESPACE, BUCKET_NAME, indexDef);
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': '1e5'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '2e3'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '20'}")
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$gte", new BsonString("")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        QueryOptions options = QueryOptions.builder()
                .sortByField("name")
                .sortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"1e5\"}",
                "{\"name\": \"2e3\"}",
                "{\"name\": \"10\"}",
                "{\"name\": \"20\"}"
        );
        assertEquals(expectedResult, actualResult,
                "ICU numeric ordering does not interpret exponent notation. 'e' is a literal separator; digit groups are compared independently: 1 < 2 (first group), so 1e5 < 2e3. Both sort before '10' and '20' because 'e' precedes digit-only strings");
    }

    // --- Collation mismatch with rewritePhysicalTrue / sortBy ---

    @Test
    void shouldNotUseCollationMismatchedIndexForSortInEmptyQuery() {
        // Behavior: Empty query with SORTBY on a collation-mismatched STRING index should
        // be rejected — the index cannot provide correct sort ordering for the requested collation.

        final String BUCKET_NAME = "test-empty-query-sort-collation-mismatch";

        Collation turkishCollation = Collation.create("tr", 1, null, null, null, null, null, null, null);
        Collation englishCollation = Collation.create("en", 1, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, turkishCollation, "name", englishCollation);

        insertDocumentsAndGetObjectIds(BUCKET_NAME, List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'zebra'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'ankara'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'istanbul'}")
        ));

        BsonDocument query = new BsonDocument();

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> createPlanWithParams(metadata, BSONUtil.toBytes(query), "name", turkishCollation));
        assertTrue(ex.getMessage().contains("collation does not match the query collation"));
    }

    @Test
    void shouldUseMatchingCollationIndexForSortInEmptyQuery() {
        // Behavior: Empty query with SORTBY on a collation-matching STRING index produces
        // a RangeScanNode — the index provides correct sort ordering.

        final String BUCKET_NAME = "test-empty-query-sort-collation-match";

        Collation frenchCollation = Collation.create("fr", 1, null, null, null, null, null, null, null);
        BucketMetadata metadata = createBucketWithCollationAndIndex(
                BUCKET_NAME, frenchCollation, "name", frenchCollation);

        insertDocumentsAndGetObjectIds(BUCKET_NAME, List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'eclair'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'banana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'fig'}")
        ));

        BsonDocument query = new BsonDocument();

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query), "name", frenchCollation);
        assertInstanceOf(RangeScanNode.class, planWithParams.plan(),
                "Matching collation should produce RangeScanNode for index-backed sort");
    }

    @Test
    void shouldNotUseBinaryIndexForCollatedSortInEmptyQuery() {
        // Behavior: Empty query with SORTBY on a binary-ordered STRING index (null collation)
        // should be rejected when query specifies a locale-aware collation.

        final String BUCKET_NAME = "test-empty-query-sort-binary-vs-collation";

        createBucket(BUCKET_NAME);
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create(
                "name_1", "name", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(TEST_NAMESPACE, BUCKET_NAME, indexDef);
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        insertDocumentsAndGetObjectIds(BUCKET_NAME, List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'zebra'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'ankara'}")
        ));

        Collation turkishCollation = Collation.create("tr", 1, null, null, null, null, null, null, null);
        BsonDocument query = new BsonDocument();

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> createPlanWithParams(metadata, BSONUtil.toBytes(query), "name", turkishCollation));
        assertTrue(ex.getMessage().contains("collation does not match the query collation"));
    }

    @Test
    void shouldUseNonStringIndexForSortInEmptyQueryRegardlessOfCollation() {
        // Behavior: Non-string (INT32) index is unaffected by query collation —
        // empty query with SORTBY on INT32 index produces RangeScanNode.

        final String BUCKET_NAME = "test-empty-query-sort-nonstring-collation";

        createBucket(BUCKET_NAME);
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create(
                "age_1", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(TEST_NAMESPACE, BUCKET_NAME, indexDef);
        BucketMetadata metadata = getBucketMetadata(BUCKET_NAME);

        insertDocumentsAndGetObjectIds(BUCKET_NAME, List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20}")
        ));

        Collation turkishCollation = Collation.create("tr", 1, null, null, null, null, null, null, null);
        BsonDocument query = new BsonDocument();

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query), "age", turkishCollation);
        assertInstanceOf(RangeScanNode.class, planWithParams.plan(),
                "Non-string index should produce RangeScanNode regardless of query collation");
    }

    @Test
    void shouldUseCompoundIndexCollationForResidualStringField() {
        // Behavior: when a compound index carries Turkish PRIMARY collation and the "name"
        // field falls to residual evaluation because the compound index is unusable (query
        // lacks leading field "status"), the compound index collation is applied instead of
        // the bucket's English PRIMARY collation. Turkish PRIMARY: dotted-i (i/İ) ≠ dotless-i
        // (ı/I), so querying "\u0130stanbul" matches 2 documents, not 3.

        final String BUCKET_NAME = "test-compound-collation-residual";

        Collation englishPrimary = Collation.create("en", 1, null, null, null, null, null, null, null);
        Collation turkishPrimary = Collation.create("tr", 1, null, null, null, null, null, null, null);

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_status_name", List.of(
                new CompoundIndexField("status", BsonType.STRING, false),
                new CompoundIndexField("name", BsonType.STRING, false)
        ), IndexStatus.WAITING, turkishPrimary);

        BucketMetadata metadata = createBucketWithCollationAndCompoundIndex(BUCKET_NAME, englishPrimary, compoundIdx);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'istanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '\u0130stanbul'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': '\u0131stanbul'}")  // dotless ı (U+0131)
        );

        insertDocumentsAndGetObjectIds(BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument("name", new BsonDocument("$eq", new BsonString("\u0130stanbul")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        assertInstanceOf(FullScanNode.class, planWithParams.plan(),
                "Query on 'name' alone must use FullScanNode — compound index requires leading 'status' field");

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(),
                    "Turkish PRIMARY (compound index): dotted i\u2261\u0130, but dotless \u0131\u2260\u0130 \u2192 2 matches. English PRIMARY (bucket) would give 3.");
            assertEquals(Set.of("istanbul", "\u0130stanbul"), extractNamesFromResults(results));
        }
    }
}
