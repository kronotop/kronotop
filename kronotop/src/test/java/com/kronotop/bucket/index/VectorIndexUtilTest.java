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

package com.kronotop.bucket.index;

import com.apple.foundationdb.Transaction;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.VectorFieldValidationException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VectorIndexUtilTest extends BaseIndexMaintainerTest {

    private static final String SELECTOR = "embedding";
    private static final int DIMENSIONS = 3;

    private static VectorIndexDefinition definition() {
        String name = VectorIndexNameGenerator.generate(SELECTOR, DIMENSIONS, DistanceFunction.COSINE);
        return VectorIndexDefinition.create(name, SELECTOR, DIMENSIONS, DistanceFunction.COSINE, IndexStatus.WAITING);
    }

    private static BsonArray validVector() {
        return new BsonArray(List.of(new BsonDouble(0.1), new BsonDouble(0.2), new BsonDouble(0.3)));
    }

    private BucketMetadata createVectorIndexAndLoadBucketMetadata() {
        createBucket(TEST_BUCKET);
        try (Transaction tr = context.getTransactionManager().create()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            VectorIndexUtil.create(tx, getBucketMetadata(TEST_BUCKET), definition());
            tr.commit().join();
        }
        return refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
    }

    @Test
    void shouldReturnFalseForNullValue() {
        assertFalse(VectorIndexUtil.validateVectorField(definition(), null));
    }

    @Test
    void shouldReturnFalseForBsonNull() {
        assertFalse(VectorIndexUtil.validateVectorField(definition(), BsonNull.VALUE));
    }

    @Test
    void shouldReturnTrueForValidVector() {
        assertTrue(VectorIndexUtil.validateVectorField(definition(), validVector()));
    }

    @Test
    void shouldRejectNonArray() {
        VectorFieldValidationException ex = assertThrows(VectorFieldValidationException.class, () ->
                VectorIndexUtil.validateVectorField(definition(), new BsonString("bad"))
        );
        assertEquals("Vector field 'embedding' must be an array", ex.getMessage());
    }

    @Test
    void shouldRejectWrongDimensions() {
        BsonArray array = new BsonArray(List.of(new BsonDouble(0.1), new BsonDouble(0.2)));
        VectorFieldValidationException ex = assertThrows(VectorFieldValidationException.class, () ->
                VectorIndexUtil.validateVectorField(definition(), array)
        );
        assertEquals("Vector field 'embedding' requires 3 dimensions but got 2", ex.getMessage());
    }

    @Test
    void shouldRejectNonDoubleElements() {
        BsonArray array = new BsonArray(List.of(new BsonDouble(0.1), new BsonInt32(2), new BsonDouble(0.3)));
        VectorFieldValidationException ex = assertThrows(VectorFieldValidationException.class, () ->
                VectorIndexUtil.validateVectorField(definition(), array)
        );
        assertEquals("Vector field 'embedding' must contain only double elements", ex.getMessage());
    }

    @Test
    void shouldReturnFalseWhenNoVectorIndex() {
        createBucket(TEST_BUCKET);
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        BsonDocument document = new BsonDocument(SELECTOR, validVector());
        assertFalse(VectorIndexUtil.validateVectorFields(metadata, document));
    }

    @Test
    void shouldReturnFalseWhenDocumentHasNoVectorField() {
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        BsonDocument document = new BsonDocument("name", new BsonString("x"));
        assertFalse(VectorIndexUtil.validateVectorFields(metadata, document));
    }

    @Test
    void shouldReturnTrueWhenDocumentHasVectorField() {
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        BsonDocument document = new BsonDocument(SELECTOR, validVector());
        assertTrue(VectorIndexUtil.validateVectorFields(metadata, document));
    }

    @Test
    void shouldPropagateValidationError() {
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        BsonDocument document = new BsonDocument(SELECTOR, new BsonString("bad"));
        VectorFieldValidationException ex = assertThrows(VectorFieldValidationException.class, () ->
                VectorIndexUtil.validateVectorFields(metadata, document)
        );
        assertEquals("Vector field 'embedding' must be an array", ex.getMessage());
    }

    @Test
    void hasVectorFieldShouldReturnFalseWhenNoVectorIndex() {
        createBucket(TEST_BUCKET);
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        BsonDocument document = new BsonDocument(SELECTOR, validVector());
        assertFalse(VectorIndexUtil.hasVectorField(metadata, document));
    }

    @Test
    void hasVectorFieldShouldReturnFalseWhenFieldMissing() {
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        BsonDocument document = new BsonDocument("name", new BsonString("x"));
        assertFalse(VectorIndexUtil.hasVectorField(metadata, document));
    }

    @Test
    void hasVectorFieldShouldReturnFalseWhenFieldIsNull() {
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        BsonDocument document = new BsonDocument(SELECTOR, BsonNull.VALUE);
        assertFalse(VectorIndexUtil.hasVectorField(metadata, document));
    }

    @Test
    void hasVectorFieldShouldReturnTrueWhenFieldPresent() {
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        BsonDocument document = new BsonDocument(SELECTOR, validVector());
        assertTrue(VectorIndexUtil.hasVectorField(metadata, document));
    }

    @Test
    void hasVectorFieldShouldNotValidate() {
        BucketMetadata metadata = createVectorIndexAndLoadBucketMetadata();
        BsonDocument document = new BsonDocument(SELECTOR, new BsonString("bad"));
        assertTrue(VectorIndexUtil.hasVectorField(metadata, document));
    }
}
