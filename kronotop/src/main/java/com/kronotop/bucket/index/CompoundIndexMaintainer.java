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

package com.kronotop.bucket.index;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.*;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Maintains compound indexes in FoundationDB for Bucket data structure.
 *
 * <p>Compound indexes use concatenated multi-field keys with the following key structures:
 * <ul>
 *   <li>Index entries: {@code (ENTRIES, val1, val2, ..., valN, ObjectId) → IndexEntry}</li>
 *   <li>Back pointers: {@code (BACK_POINTER, ObjectId, val1, val2, ..., valN) → null}</li>
 * </ul>
 */
public final class CompoundIndexMaintainer extends IndexMaintainer {

    /**
     * Builds the entry key tuple: (ENTRIES, val1, ..., valN, objectId).
     */
    private static Tuple buildEntryTuple(List<Object> fieldValues, byte[] objectId, Collation collation, CollatorCache collatorCache) {
        Object[] items = new Object[fieldValues.size() + 2];
        items[0] = IndexSubspaceMagic.ENTRIES.getValue();
        for (int i = 0; i < fieldValues.size(); i++) {
            Object val = fieldValues.get(i);
            if (val instanceof ObjectId oid) {
                val = oid.toByteArray();
            }
            val = applyCollation(val, collation, collatorCache);
            items[i + 1] = val;
        }
        items[items.length - 1] = objectId;
        return Tuple.from(items);
    }

    /**
     * Builds the back pointer key tuple: (BACK_POINTER, objectId, val1, ..., valN).
     */
    private static Tuple buildBackPointerTuple(List<Object> fieldValues, byte[] objectId, Collation collation, CollatorCache collatorCache) {
        Object[] items = new Object[fieldValues.size() + 2];
        items[0] = IndexSubspaceMagic.BACK_POINTER.getValue();
        items[1] = objectId;
        for (int i = 0; i < fieldValues.size(); i++) {
            Object val = fieldValues.get(i);
            if (val instanceof ObjectId oid) {
                val = oid.toByteArray();
            }
            val = applyCollation(val, collation, collatorCache);
            items[i + 2] = val;
        }
        return Tuple.from(items);
    }

    /**
     * Creates a compound index entry with associated back pointer and watermark.
     *
     * @param tr            the FoundationDB transaction
     * @param compoundIndex the resolved compound index
     * @param metadata      the bucket metadata
     * @param fieldValues   ordered list of field values for the compound key
     * @param objectId      the document's ObjectId as bytes
     * @param indexEntry    the pre-encoded IndexEntry bytes
     * @param userVersion   the user version for the incomplete versionstamp
     * @param collatorCache cache for collation-aware key encoding
     */
    public static void setEntry(
            Transaction tr,
            CompoundIndex compoundIndex,
            BucketMetadata metadata,
            List<Object> fieldValues,
            byte[] objectId,
            byte[] indexEntry,
            int userVersion,
            CollatorCache collatorCache
    ) {
        Collation collation = resolveCollation(compoundIndex.definition(), metadata);
        Tuple entryTuple = buildEntryTuple(fieldValues, objectId, collation, collatorCache);
        byte[] key = compoundIndex.subspace().pack(entryTuple);
        tr.set(key, indexEntry);

        IndexUtil.mutateCardinality(tr, metadata.subspace(), compoundIndex.definition().id(), 1);

        Tuple backPointerTuple = buildBackPointerTuple(fieldValues, objectId, collation, collatorCache);
        byte[] backPointer = compoundIndex.subspace().pack(backPointerTuple);
        tr.set(backPointer, NULL_VALUE);

        if (compoundIndex.definition().status() != IndexStatus.READY) {
            addWatermark(tr, compoundIndex.subspace(), userVersion);
        }
    }

    /**
     * Inserts a compound index entry with an explicit versionstamp and watermark.
     *
     * @param tr            the FoundationDB transaction
     * @param compoundIndex the resolved compound index
     * @param metadata      the bucket metadata
     * @param versionstamp  the document's versionstamp
     * @param objectId      the document's ObjectId as bytes
     * @param fieldValues   ordered list of field values for the compound key
     * @param shardId       the shard containing the document
     * @param entryMetadata the encoded entry metadata
     * @param collatorCache cache for collation-aware key encoding
     */
    public static void insertEntry(
            Transaction tr,
            CompoundIndex compoundIndex,
            BucketMetadata metadata,
            Versionstamp versionstamp,
            byte[] objectId,
            List<Object> fieldValues,
            int shardId,
            byte[] entryMetadata,
            CollatorCache collatorCache
    ) {
        Collation collation = resolveCollation(compoundIndex.definition(), metadata);
        Tuple entryTuple = buildEntryTuple(fieldValues, objectId, collation, collatorCache);
        byte[] key = compoundIndex.subspace().pack(entryTuple);
        IndexEntry indexEntry = new IndexEntry(shardId, entryMetadata);
        tr.set(key, indexEntry.encode());

        IndexUtil.mutateCardinality(tr, metadata.subspace(), compoundIndex.definition().id(), 1);

        Tuple backPointerTuple = buildBackPointerTuple(fieldValues, objectId, collation, collatorCache);
        byte[] backPointer = compoundIndex.subspace().pack(backPointerTuple);
        tr.set(backPointer, NULL_VALUE);

        if (compoundIndex.definition().status() != IndexStatus.READY) {
            addWatermark(tr, compoundIndex.subspace(), versionstamp);
        }
    }

    /**
     * Reconstructs the ENTRIES key from a back pointer tuple.
     * Back pointer layout: (BACK_POINTER, objectId, val1, ..., valN)
     * Entry layout: (ENTRIES, val1, ..., valN, objectId)
     */
    private static byte[] rebuildEntryKey(DirectorySubspace indexSubspace, Tuple backPointerTuple, byte[] objectId, int fieldCount) {
        Object[] entryItems = new Object[fieldCount + 2];
        entryItems[0] = IndexSubspaceMagic.ENTRIES.getValue();
        for (int i = 0; i < fieldCount; i++) {
            entryItems[i + 1] = backPointerTuple.get(2 + i);
        }
        entryItems[fieldCount + 1] = objectId;
        return indexSubspace.pack(Tuple.from(entryItems));
    }

    /**
     * Removes all compound index entries and back pointers for a document.
     *
     * @param tr               the FoundationDB transaction
     * @param objectId         the document's ObjectId as bytes
     * @param definition       the compound index definition
     * @param indexSubspace    the index's directory subspace
     * @param metadataSubspace the bucket's metadata subspace for cardinality updates
     */
    public static void dropEntry(
            Transaction tr,
            byte[] objectId,
            CompoundIndexDefinition definition,
            DirectorySubspace indexSubspace,
            DirectorySubspace metadataSubspace
    ) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), objectId));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        int fieldCount = definition.fields().size();
        long total = 0;
        for (KeyValue kv : tr.getRange(begin, end)) {
            Tuple unpacked = indexSubspace.unpack(kv.getKey());
            byte[] indexKey = rebuildEntryKey(indexSubspace, unpacked, objectId, fieldCount);
            tr.clear(indexKey);
            total--;
        }

        IndexUtil.mutateCardinality(tr, metadataSubspace, definition.id(), total);
        tr.clear(begin.getKey(), end.getKey());
    }

    /**
     * Updates the index entry value for all existing back pointers of a document without modifying the key structure.
     *
     * @param tr            the FoundationDB transaction
     * @param objectId      the document's ObjectId as bytes
     * @param indexEntry    the new encoded IndexEntry bytes
     * @param indexSubspace the index's directory subspace
     * @param fieldCount    the number of fields in the compound index
     */
    public static void updateIndexEntry(
            Transaction tr,
            byte[] objectId,
            byte[] indexEntry,
            DirectorySubspace indexSubspace,
            int fieldCount
    ) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), objectId));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        List<KeyValue> allBackPointers = tr.getRange(begin, end).asList().join();
        for (KeyValue kv : allBackPointers) {
            Tuple unpacked = indexSubspace.unpack(kv.getKey());
            byte[] indexKey = rebuildEntryKey(indexSubspace, unpacked, objectId, fieldCount);
            tr.set(indexKey, indexEntry);
        }
    }

    /**
     * Extracts field value combinations for a compound index from a BsonDocument.
     * When a multi-key field contains an array, it produces one value-list per unique array element.
     *
     * @param compoundIndex the compound index definition
     * @param document      the source document
     * @param strictTypes   whether to throw on type mismatch
     * @return list of value combinations; an empty list means the index should be skipped for this document
     */
    public static List<List<Object>> extractFieldValues(CompoundIndex compoundIndex, BsonDocument document, boolean strictTypes) {
        List<CompoundIndexField> fields = compoundIndex.definition().fields();
        Object[] baseValues = new Object[fields.size()];
        int multiKeyFieldIndex = -1;
        Set<Object> multiKeyValues = null;

        for (int f = 0; f < fields.size(); f++) {
            CompoundIndexField field = fields.get(f);
            BsonValue bsonValue = SelectorMatcher.match(field.selector(), document);
            FieldResult result = processFieldValue(compoundIndex, field, bsonValue, strictTypes);
            if (result == null) {
                return List.of();
            }
            if (result.isMultiKey) {
                multiKeyFieldIndex = f;
                multiKeyValues = result.multiKeyValues;
            } else {
                baseValues[f] = result.singleValue;
            }
        }

        return buildCombinations(baseValues, multiKeyFieldIndex, multiKeyValues);
    }

    /**
     * Extracts field value combinations for a compound index from a ByteBuffer document.
     * Rewinds the buffer after each field extraction.
     *
     * @param compoundIndex the compound index definition
     * @param document      the source document as ByteBuffer
     * @param strictTypes   whether to throw on type mismatch
     * @return list of value combinations; an empty list means the index should be skipped for this document
     */
    public static List<List<Object>> extractFieldValues(CompoundIndex compoundIndex, ByteBuffer document, boolean strictTypes) {
        List<CompoundIndexField> fields = compoundIndex.definition().fields();
        Object[] baseValues = new Object[fields.size()];
        int multiKeyFieldIndex = -1;
        Set<Object> multiKeyValues = null;

        for (int f = 0; f < fields.size(); f++) {
            CompoundIndexField field = fields.get(f);
            BsonValue bsonValue = SelectorMatcher.match(field.selector(), document);
            document.rewind();
            FieldResult result = processFieldValue(compoundIndex, field, bsonValue, strictTypes);
            if (result == null) {
                return List.of();
            }
            if (result.isMultiKey) {
                multiKeyFieldIndex = f;
                multiKeyValues = result.multiKeyValues;
            } else {
                baseValues[f] = result.singleValue;
            }
        }

        return buildCombinations(baseValues, multiKeyFieldIndex, multiKeyValues);
    }

    /**
     * Processes a single field's BsonValue for compound index extraction.
     * Returns null if the entire index should be skipped for this document (type mismatch in non-strict mode).
     */
    private static FieldResult processFieldValue(CompoundIndex compoundIndex, CompoundIndexField field, BsonValue bsonValue, boolean strictTypes) {
        if (field.multiKey() && bsonValue instanceof BsonArray bsonArray) {
            Set<Object> values = new LinkedHashSet<>();
            boolean hasNull = false;
            for (BsonValue element : bsonArray) {
                if (element == null || element.equals(BsonNull.VALUE)) {
                    hasNull = true;
                    continue;
                }
                Object val = BSONUtil.toObject(element, field.bsonType());
                if (val == null) {
                    if (strictTypes) {
                        throw new IndexTypeMismatchException(formatMismatchMessage(compoundIndex, field, element));
                    }
                    continue;
                }
                values.add(val);
            }
            if (hasNull) {
                values.add(null);
            }
            return FieldResult.multiKey(values);
        }

        Object val = null;
        if (bsonValue != null && !bsonValue.equals(BsonNull.VALUE)) {
            val = BSONUtil.toObject(bsonValue, field.bsonType());
            if (val == null) {
                if (strictTypes) {
                    throw new IndexTypeMismatchException(formatMismatchMessage(compoundIndex, field, bsonValue));
                }
                return null;
            }
        }
        return FieldResult.single(val);
    }

    private static String formatMismatchMessage(CompoundIndex compoundIndex, CompoundIndexField field, BsonValue bsonValue) {
        return String.format(
                "Index type mismatch: compound index '%s' field '%s' expects '%s', but got '%s'",
                compoundIndex.definition().name(), field.selector(), field.bsonType(), bsonValue.getBsonType()
        );
    }

    private static List<List<Object>> buildCombinations(Object[] baseValues, int multiKeyFieldIndex, Set<Object> multiKeyValues) {
        if (multiKeyFieldIndex == -1) {
            return List.of(Arrays.asList(baseValues));
        }
        if (multiKeyValues.isEmpty()) {
            return List.of();
        }
        List<List<Object>> result = new ArrayList<>(multiKeyValues.size());
        for (Object mkVal : multiKeyValues) {
            Object[] combo = baseValues.clone();
            combo[multiKeyFieldIndex] = mkVal;
            result.add(Arrays.asList(combo));
        }
        return result;
    }

    private record FieldResult(boolean isMultiKey, Object singleValue, Set<Object> multiKeyValues) {
        static FieldResult single(Object value) {
            return new FieldResult(false, value, null);
        }

        static FieldResult multiKey(Set<Object> values) {
            return new FieldResult(true, null, values);
        }
    }
}
