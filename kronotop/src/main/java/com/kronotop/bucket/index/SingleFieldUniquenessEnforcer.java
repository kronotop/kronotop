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

import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.CollatorCache;
import com.kronotop.bucket.DuplicateKeyException;
import com.kronotop.bucket.IndexTypeMismatchException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Enqueues uniqueness checks for the writable, unique single field indexes of a bucket.
 * <p>
 * One instance backs a single write operation (one INSERT or UPDATE command, which may touch many
 * documents). The checks are added to a shared {@link UniquenessChecker} whose reads run in parallel
 * and are drained before the volume write, so a violation never leaves appended bytes behind.
 * <p>
 * Two documents in the same operation cannot both hold the same value: FoundationDB reads do not see
 * a sibling that has not been written yet, so an in-memory set catches within-batch duplicates. An
 * instance is not thread-safe.
 */
public final class SingleFieldUniquenessEnforcer {
    private final BucketMetadata metadata;
    private final CollatorCache collatorCache;
    private final boolean strictTypes;
    // Each entry prefix packs the index's own subspace, so prefixes are unique across index and value.
    // A single set therefore catches within-batch duplicates for every unique index at once.
    private final Set<ByteBuffer> batchSeen = new HashSet<>();

    public SingleFieldUniquenessEnforcer(BucketMetadata metadata, CollatorCache collatorCache, boolean strictTypes) {
        this.metadata = metadata;
        this.collatorCache = collatorCache;
        this.strictTypes = strictTypes;
    }

    /**
     * Adds a uniqueness check per writable unique single field index for the given document.
     *
     * @param checker       the shared checker collecting the reads
     * @param objectIdBytes the document's ObjectId as bytes, excluded from its own value match
     * @param document      the document being written
     * @throws DuplicateKeyException      if another document in this operation already holds the value
     * @throws IndexTypeMismatchException if strict typing is on and a value does not match the index type
     */
    public void enqueue(UniquenessChecker checker, byte[] objectIdBytes, BsonDocument document) {
        for (Index index : metadata.indexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
            SingleFieldIndexDefinition definition = index.definition();
            if (!definition.unique() || PrimaryIndex.isPrimary(definition)) {
                continue;
            }
            process(checker, objectIdBytes, index, SelectorMatcher.match(definition.selector(), document));
        }
    }

    /**
     * Same as {@link #enqueue(UniquenessChecker, byte[], BsonDocument)} for a document held in a
     * ByteBuffer, used by the update path where the new document is already serialized.
     */
    public void enqueue(UniquenessChecker checker, byte[] objectIdBytes, ByteBuffer document) {
        for (Index index : metadata.indexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
            SingleFieldIndexDefinition definition = index.definition();
            if (!definition.unique() || PrimaryIndex.isPrimary(definition)) {
                continue;
            }
            process(checker, objectIdBytes, index, SelectorMatcher.match(definition.selector(), document));
        }
    }

    private void process(UniquenessChecker checker, byte[] objectIdBytes, Index index, BsonValue bsonValue) {
        SingleFieldIndexDefinition definition = index.definition();
        for (Object rawValue : indexedValues(definition, bsonValue)) {
            Object encoded = SingleFieldIndexMaintainer.encodeIndexValue(definition, metadata, rawValue, collatorCache);
            byte[] valuePrefix = SingleFieldIndexMaintainer.entryValuePrefix(index.subspace(), encoded);

            if (!batchSeen.add(ByteBuffer.wrap(valuePrefix))) {
                throw new DuplicateKeyException(definition.selector(), rawValue);
            }

            checker.checkFieldValue(index.subspace(), valuePrefix, objectIdBytes,
                    () -> new DuplicateKeyException(definition.selector(), rawValue));
        }
    }

    /**
     * Returns the raw values this document contributes to the index, mirroring exactly what the write
     * path stores (see {@code BucketInsertHandler.setSingleFieldIndexes}). An array field yields one
     * value per distinct element, so uniqueness covers every entry that is actually written. Missing
     * and explicit null are indexed as null and count as values.
     */
    private List<Object> indexedValues(SingleFieldIndexDefinition definition, BsonValue bsonValue) {
        if (bsonValue instanceof BsonArray bsonArray) {
            List<Object> values = new ArrayList<>();
            Set<Object> distinct = new HashSet<>();
            boolean hasNull = false;
            for (BsonValue element : bsonArray) {
                if (element == null || element.equals(BsonNull.VALUE)) {
                    hasNull = true;
                    continue;
                }
                Object value = BSONUtil.toObject(element, definition.bsonType());
                if (value == null) {
                    // Type mismatch. In non-strict mode this element is not written, so skip it.
                    if (strictTypes) {
                        throw new IndexTypeMismatchException(definition, element);
                    }
                    continue;
                }
                if (distinct.add(value)) {
                    values.add(value);
                }
            }
            if (hasNull && distinct.add(null)) {
                values.add(null);
            }
            return values;
        }

        if (bsonValue == null || bsonValue.equals(BsonNull.VALUE)) {
            return java.util.Collections.singletonList(null);
        }

        Object value = BSONUtil.toObject(bsonValue, definition.bsonType());
        if (value == null) {
            // Type mismatch. In non-strict mode no entry is written, so nothing to enforce.
            if (strictTypes) {
                throw new IndexTypeMismatchException(definition, bsonValue);
            }
            return List.of();
        }
        return List.of(value);
    }
}
