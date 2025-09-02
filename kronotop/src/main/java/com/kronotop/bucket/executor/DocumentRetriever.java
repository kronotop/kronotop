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

package com.kronotop.bucket.executor;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.BucketShard;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.index.IndexEntry;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.volume.EntryMetadata;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Handles document retrieval operations from storage.
 * Extracted from PlanExecutor to provide focused document retrieval functionality.
 */
public class DocumentRetriever {
    private final BucketService bucketService;

    DocumentRetriever(BucketService bucketService) {
        this.bucketService = bucketService;
    }

    /**
     * Extracts document location information from an index entry.
     *
     * @param idIndexSubspace the ID index directory subspace
     * @param indexEntry      the key-value pair from the index scan
     * @return document location containing ID, shard, and metadata
     */
    DocumentLocation extractDocumentLocation(DirectorySubspace idIndexSubspace, KeyValue indexEntry) {
        // Extract the Versionstamp from the index key
        Tuple indexKeyTuple = idIndexSubspace.unpack(indexEntry.getKey());
        Versionstamp documentId = (Versionstamp) indexKeyTuple.get(1); // Skip ENTRIES_MAGIC, get Versionstamp

        // Decode the IndexEntry from the value
        IndexEntry indexEntryData = IndexEntry.decode(indexEntry.getValue());
        int shardId = indexEntryData.shardId();
        EntryMetadata entryMetadata = EntryMetadata.decode(ByteBuffer.wrap(indexEntryData.entryMetadata()));

        return new DocumentLocation(documentId, shardId, entryMetadata);
    }

    /**
     * Extracts document location information from a regular index scan entry.
     * For regular indexes, the key structure is [ENTRIES_MAGIC, indexed_value, Versionstamp]
     *
     * @param indexSubspace the index directory subspace
     * @param indexEntry    the key-value pair from the index scan
     * @return document location containing ID, shard, and metadata
     */
    DocumentLocation extractDocumentLocationFromIndexScan(DirectorySubspace indexSubspace, KeyValue indexEntry) {
        // Extract the Versionstamp from the index key
        Tuple indexKeyTuple = indexSubspace.unpack(indexEntry.getKey());
        Versionstamp documentId;

        // Handle different index structures:
        // Regular indexes: (ENTRIES_MAGIC, indexed_value, Versionstamp) - 3 elements
        // _id index: (ENTRIES_MAGIC, Versionstamp) - 2 elements (Versionstamp is both indexed value and document ID)
        if (indexKeyTuple.size() == 3) {
            documentId = (Versionstamp) indexKeyTuple.get(2); // Regular index: get Versionstamp from position 2
        } else if (indexKeyTuple.size() == 2) {
            documentId = (Versionstamp) indexKeyTuple.get(1); // _id index: get Versionstamp from position 1
        } else {
            throw new IllegalStateException("Unexpected index key tuple size: " + indexKeyTuple.size());
        }

        // Decode the IndexEntry from the value
        IndexEntry indexEntryData = IndexEntry.decode(indexEntry.getValue());
        int shardId = indexEntryData.shardId();
        EntryMetadata entryMetadata = EntryMetadata.decode(ByteBuffer.wrap(indexEntryData.entryMetadata()));

        return new DocumentLocation(documentId, shardId, entryMetadata);
    }

    /**
     * Retrieves a document from its storage location.
     *
     * @param metadata the bucket metadata
     * @param location the document location information
     * @return the document content as a ByteBuffer
     * @throws RuntimeException if shard is not found or document retrieval fails
     */
    ByteBuffer retrieveDocument(BucketMetadata metadata, DocumentLocation location) {
        BucketShard bucketShard = bucketService.getShard(location.shardId());
        if (bucketShard == null) {
            throw new RuntimeException("Shard not found for ID: " + location.shardId());
        }

        try {
            return bucketShard.volume().get(
                    metadata.volumePrefix(),
                    location.documentId(),
                    location.entryMetadata()
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to retrieve document with ID: " + location.documentId() + " from shard: " + location.shardId(), e);
        }
    }

    /**
     * Retrieves a document by its Versionstamp using the ID index.
     */
    ByteBuffer retrieveDocumentById(Transaction tr, Versionstamp versionstamp, BucketMetadata metadata) {
        DirectorySubspace idIndexSubspace = getIdIndexSubspace(metadata);
        Tuple idKey = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), versionstamp);
        byte[] indexKey = idIndexSubspace.pack(idKey);

        byte[] indexValue = tr.get(indexKey).join();
        if (indexValue != null) {
            IndexEntry indexEntryData = IndexEntry.decode(indexValue);
            EntryMetadata entryMetadata = EntryMetadata.decode(ByteBuffer.wrap(indexEntryData.entryMetadata()));
            DocumentLocation location = new DocumentLocation(versionstamp, indexEntryData.shardId(), entryMetadata);
            return retrieveDocument(metadata, location);
        }
        return null;
    }

    /**
     * Gets the ID index subspace from bucket metadata.
     *
     * @param metadata the bucket metadata
     * @return the directory subspace for the ID index
     * @throws RuntimeException if the ID index is not found
     */
    private DirectorySubspace getIdIndexSubspace(BucketMetadata metadata) {
        DirectorySubspace idIndexSubspace = metadata.indexes().getSubspace(DefaultIndexDefinition.ID.selector());
        if (idIndexSubspace == null) {
            throw new RuntimeException("ID index not found for bucket: " + metadata.name());
        }
        return idIndexSubspace;
    }

    /**
     * Record representing document location information.
     */
    public record DocumentLocation(Versionstamp documentId, int shardId, EntryMetadata entryMetadata) {
    }
}