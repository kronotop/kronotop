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
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.CollatorCache;
import com.kronotop.bucket.IndexTypeMismatchException;
import org.bson.BsonArray;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Maintains single field indexes in FoundationDB.
 *
 * <p>Single field indexes use the following key structures:
 * <ul>
 *   <li>Index entries: {@code (ENTRIES, indexValue, ObjectId) -> IndexEntry}</li>
 *   <li>Back pointers: {@code (BACK_POINTER, ObjectId, indexValue) -> null}</li>
 * </ul>
 *
 * <p>Back pointers enable efficient deletion by allowing reverse lookup from ObjectId to all
 * index values associated with that document.
 */
public final class SingleFieldIndexMaintainer extends IndexMaintainer {

    /**
     * Extracts the unique index values of an array field for a multikey index. Null elements collapse
     * into a single null value. Elements that do not match the index type are skipped, or rejected
     * with {@link IndexTypeMismatchException} when strict types are enabled.
     *
     * @param definition  the index definition
     * @param array       the array value matched by the index selector
     * @param strictTypes whether a type mismatch is an error
     * @return the deduplicated index values and the BSON elements they were derived from
     */
    public static MultikeyValues extractMultikeyValues(SingleFieldIndexDefinition definition, BsonArray array, boolean strictTypes) {
        Set<Object> indexValues = new HashSet<>();
        List<BsonValue> bsonValues = new ArrayList<>();
        boolean hasNull = false;
        for (BsonValue element : array) {
            if (element == null || element.equals(BsonNull.VALUE)) {
                hasNull = true;
                continue;
            }
            Object indexValue = BSONUtil.toObject(element, definition.bsonType());
            if (indexValue == null) {
                if (strictTypes) {
                    throw new IndexTypeMismatchException(definition, element);
                }
                continue;
            }
            if (indexValues.add(indexValue)) {
                bsonValues.add(element);
            }
        }
        // Index null elements (deduplicated) for consistent semantics with single-value indexes
        if (hasNull && indexValues.add(null)) {
            bsonValues.add(BsonNull.VALUE);
        }
        return new MultikeyValues(indexValues, bsonValues);
    }

    /**
     * Constructs the key for a single field index entry.
     *
     * @param index      the index containing the subspace
     * @param indexValue the field value being indexed
     * @param objectId   the document's ObjectId as bytes
     * @return the packed key bytes
     */
    private static byte[] getSingleFieldIndexEntryKey(SingleFieldIndex index, Object indexValue, byte[] objectId) {
        // Single Field Index Key Structure: (ENTRIES, indexValue, ObjectId)
        Tuple indexEntryTuple = Tuple.from(
                IndexSubspaceMagic.ENTRIES.getValue(),
                indexValue,
                objectId
        );
        return index.subspace().pack(indexEntryTuple);
    }

    /**
     * Encodes a raw field value into the form stored in index keys: ObjectId values become their
     * byte representation, and String values are replaced by their collation key. This is the single
     * source of truth for index value encoding, so uniqueness checks and entry writes agree exactly.
     *
     * @param definition    the index definition
     * @param metadata      the bucket metadata
     * @param rawValue      the raw field value (may be null)
     * @param collatorCache the collator cache for collation-aware encoding
     * @return the encoded index value
     */
    public static Object encodeIndexValue(
            SingleFieldIndexDefinition definition,
            BucketMetadata metadata,
            Object rawValue,
            CollatorCache collatorCache
    ) {
        if (rawValue instanceof ObjectId objectIdValue) {
            rawValue = objectIdValue.toByteArray();
        }
        Collation collation = resolveCollation(definition, metadata);
        return applyCollation(rawValue, collation, collatorCache);
    }

    /**
     * Builds the key prefix that covers every entry with the given encoded value, regardless of
     * ObjectId: {@code (ENTRIES, encodedValue)}. A range read over this prefix finds all documents
     * that share the value, which is how uniqueness is checked.
     *
     * @param indexSubspace the index's directory subspace
     * @param encodedValue  the encoded index value (see {@link #encodeIndexValue})
     * @return the packed prefix bytes
     */
    public static byte[] entryValuePrefix(DirectorySubspace indexSubspace, Object encodedValue) {
        return indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), encodedValue));
    }

    /**
     * Creates a single field index entry with an associated back pointer.
     *
     * @param tr            the FoundationDB transaction
     * @param index         the resolved index
     * @param metadata      the bucket metadata
     * @param indexValue    the field value being indexed
     * @param objectId      the document's ObjectId as bytes
     * @param indexEntry    the pre-encoded IndexEntry bytes
     * @param collatorCache the collator cache for collation-aware indexing
     */
    public static void setEntry(
            Transaction tr,
            SingleFieldIndex index,
            BucketMetadata metadata,
            Object indexValue,
            byte[] objectId,
            byte[] indexEntry,
            CollatorCache collatorCache
    ) {
        indexValue = encodeIndexValue(index.definition(), metadata, indexValue, collatorCache);

        byte[] key = getSingleFieldIndexEntryKey(index, indexValue, objectId);
        tr.set(key, indexEntry);

        IndexUtil.mutateCardinality(tr, metadata.subspace(), index.definition().id(), 1);

        // Back pointer: (BACK_POINTER, ObjectId, indexValue)
        Tuple backPointerTuple = Tuple.from(
                IndexSubspaceMagic.BACK_POINTER.getValue(),
                objectId,
                indexValue
        );
        byte[] backPointer = index.subspace().pack(backPointerTuple);
        tr.set(backPointer, NULL_VALUE);
    }

    /**
     * Inserts a single field index entry, building the IndexEntry from the supplied shard and metadata.
     *
     * <p>Unlike {@link #setEntry}, which takes an already encoded {@link IndexEntry}, this method builds
     * the entry from the given shard and entry metadata.
     *
     * @param tr            the FoundationDB transaction
     * @param index         the resolved index
     * @param metadata      the bucket metadata
     * @param objectId      the document's ObjectId as bytes
     * @param indexValue    the field value being indexed
     * @param shardId       the shard containing the document
     * @param entry         the encoded entry metadata
     * @param collatorCache the collator cache for collation-aware indexing
     */
    public static void insertEntry(
            Transaction tr,
            SingleFieldIndex index,
            BucketMetadata metadata,
            byte[] objectId,
            Object indexValue,
            int shardId,
            byte[] entry,
            CollatorCache collatorCache
    ) {
        indexValue = encodeIndexValue(index.definition(), metadata, indexValue, collatorCache);
        byte[] key = getSingleFieldIndexEntryKey(index, indexValue, objectId);

        IndexEntry indexEntry = new IndexEntry(shardId, entry);
        tr.set(key, indexEntry.encode());

        IndexUtil.mutateCardinality(tr, metadata.subspace(), index.definition().id(), 1);

        Tuple backPointerTuple = Tuple.from(
                IndexSubspaceMagic.BACK_POINTER.getValue(),
                objectId,
                indexValue
        );
        byte[] backPointer = index.subspace().pack(backPointerTuple);
        tr.set(backPointer, NULL_VALUE);
    }

    /**
     * Removes all index data for a document, including entries and back pointers.
     *
     * <p>Uses back pointers to find all indexed values for the document, then clears the
     * corresponding index entries and updates cardinality.
     *
     * @param tr               the FoundationDB transaction
     * @param objectId         the document's ObjectId as bytes
     * @param definition       the index definition
     * @param indexSubspace    the index's directory subspace
     * @param metadataSubspace the bucket's metadata subspace for cardinality updates
     */
    public static void dropEntry(
            Transaction tr,
            byte[] objectId,
            SingleFieldIndexDefinition definition,
            DirectorySubspace indexSubspace,
            DirectorySubspace metadataSubspace
    ) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), objectId));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        // Drop index keys
        long total = 0;
        for (KeyValue kv : tr.getRange(begin, end)) {
            Tuple unpacked = indexSubspace.unpack(kv.getKey());
            Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), unpacked.get(2), objectId);
            byte[] indexKey = indexSubspace.pack(tuple);
            tr.clear(indexKey);
            total--;
        }
        IndexUtil.mutateCardinality(tr, metadataSubspace, definition.id(), total);
        // Drop the back pointers
        tr.clear(begin.getKey(), end.getKey());
    }

    /**
     * Updates the entry metadata for all index entries of a document without changing the indexed keys.
     *
     * <p>Uses back pointers to locate all index entries for the document, then overwrites
     * their values with the new IndexEntry bytes.
     *
     * @param tr            the FoundationDB transaction
     * @param objectId      the document's ObjectId as bytes
     * @param indexEntry    the new encoded IndexEntry bytes
     * @param indexSubspace the index's directory subspace
     */
    public static void updateIndexEntry(
            Transaction tr,
            byte[] objectId,
            byte[] indexEntry,
            DirectorySubspace indexSubspace
    ) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), objectId));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        List<KeyValue> allBackPointers = tr.getRange(begin, end).asList().join();
        for (KeyValue kv : allBackPointers) {
            Tuple unpacked = indexSubspace.unpack(kv.getKey());
            Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), unpacked.get(2), objectId);
            byte[] key = indexSubspace.pack(tuple);
            tr.set(key, indexEntry);
        }
    }

    /**
     * Creates a single field index entry using an {@link IndexEntryContainer}.
     *
     * <p>Convenience method that extracts all required values from the container and creates
     * the index entry with back pointer and cardinality update.
     *
     * @param tr            the FoundationDB transaction
     * @param objectId      the document's ObjectId as bytes
     * @param container     the container holding index value, subspace, metadata, and shard information
     * @param collatorCache the collator cache for collation-aware indexing
     */
    public static void setEntryByObjectId(Transaction tr, byte[] objectId, IndexEntryContainer container, CollatorCache collatorCache) {
        Object indexValue = encodeIndexValue(container.indexDefinition(), container.metadata(), container.indexValue(), collatorCache);

        Tuple indexKeyTuple = Tuple.from(
                IndexSubspaceMagic.ENTRIES.getValue(),
                indexValue,
                objectId
        );

        byte[] key = container.indexSubspace().pack(indexKeyTuple);
        IndexEntry indexEntry = new IndexEntry(container.shardId(), container.entryMetadata());
        tr.set(key, indexEntry.encode());

        Tuple backPointerTuple = Tuple.from(
                IndexSubspaceMagic.BACK_POINTER.getValue(),
                objectId,
                indexValue
        );

        byte[] backPointer = container.indexSubspace().pack(backPointerTuple);
        tr.set(backPointer, NULL_VALUE);

        IndexUtil.mutateCardinality(tr, container.metadata().subspace(), container.indexDefinition().id(), 1);
    }

    /**
     * Deduplicated values of an array field for a multikey index. {@code indexValues} are the converted
     * values to write as index entries. {@code bsonValues} are the source elements, one per index value,
     * used for statistics hints.
     */
    public record MultikeyValues(Set<Object> indexValues, List<BsonValue> bsonValues) {
    }
}
