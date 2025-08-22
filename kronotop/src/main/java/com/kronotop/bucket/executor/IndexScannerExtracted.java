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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexEntry;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.planner.physical.PhysicalFilter;
import com.kronotop.volume.EntryMetadata;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Scanner for continuously fetching entries from an index in batches.
 * Accumulates EntryMetadata IDs for intersection and manages cursor advancement.
 * Extracted from PlanExecutor inner class to be a standalone component.
 */
class IndexScannerExtracted {
    private final Transaction tr;
    private final DirectorySubspace indexSubspace;
    private final IndexDefinition indexDefinition;
    private final boolean reverse;
    private final CursorManager cursorManager;
    private final IndexUtils indexUtils;
    private final PhysicalFilter filter;

    private final RoaringBitmap accumulatedEntryIds = new RoaringBitmap();
    private final Map<Versionstamp, Integer> versionstampToEntryId = new HashMap<>();

    private KeySelector currentBeginSelector;
    private KeySelector endSelector;
    private boolean hasMore = true;

    // Track last processed values for cursor management
    private BqlValue lastProcessedIndexValue;
    private Versionstamp lastProcessedVersionstamp;

    IndexScannerExtracted(Transaction tr, DirectorySubspace indexSubspace, IndexDefinition indexDefinition,
                          PhysicalFilter filter, boolean reverse, CursorManager cursorManager, IndexUtils indexUtils) {
        this.tr = tr;
        this.indexSubspace = indexSubspace;
        this.indexDefinition = indexDefinition;
        this.reverse = reverse;
        this.cursorManager = cursorManager;
        this.indexUtils = indexUtils;
        this.filter = filter;

        // Initialize selectors considering cursor state
        initializeSelectorsWithCursor(filter);
    }

    private void initializeSelectorsWithCursor(PhysicalFilter filter) {
        // For now, always start from the beginning and let the intersection logic handle cursoring
        // This is simpler and more reliable than trying to coordinate cursors across multiple indexes
        KeySelector[] selectors = indexUtils.constructIndexRangeSelectors(indexSubspace, filter, indexDefinition);
        this.currentBeginSelector = selectors[0];
        this.endSelector = selectors[1];
    }

    boolean hasMore() {
        return hasMore;
    }

    PhysicalFilter getFilter() {
        return filter;
    }

    void fetchNextBatch(int batchSize) {
        if (!hasMore) {
            return;
        }

        AsyncIterable<KeyValue> indexEntries = tr.getRange(currentBeginSelector, endSelector, batchSize, reverse);
        boolean foundEntries = false;

        for (KeyValue indexEntry : indexEntries) {
            foundEntries = true;

            // Extract document location from index entry
            Tuple indexKeyTuple = indexSubspace.unpack(indexEntry.getKey());
            Versionstamp versionstamp;
            Object rawIndexValue;

            // Handle different index structures:
            // Regular indexes: (ENTRIES_MAGIC, indexed_value, Versionstamp) - 3 elements
            // _id index: (ENTRIES_MAGIC, Versionstamp) - 2 elements
            if (indexKeyTuple.size() == 3) {
                versionstamp = (Versionstamp) indexKeyTuple.get(2); // Regular index: versionstamp at position 2
                rawIndexValue = indexKeyTuple.get(1); // indexed_value at position 1
            } else if (indexKeyTuple.size() == 2) {
                versionstamp = (Versionstamp) indexKeyTuple.get(1); // _id index: versionstamp at position 1
                rawIndexValue = versionstamp; // For _id index, the versionstamp IS the indexed value
            } else {
                throw new IllegalStateException("Unexpected index key tuple size: " + indexKeyTuple.size());
            }

            // Decode index entry data
            IndexEntry indexEntryData = IndexEntry.decode(indexEntry.getValue());
            EntryMetadata entryMetadata = EntryMetadata.decode(ByteBuffer.wrap(indexEntryData.entryMetadata()));

            // Add to accumulated results
            int entryId = entryMetadata.id();
            accumulatedEntryIds.add(entryId);
            versionstampToEntryId.put(versionstamp, entryId);

            // Track last processed values for the next batch cursor advancement
            lastProcessedIndexValue = cursorManager.createBqlValueFromIndexValue(rawIndexValue, indexDefinition.bsonType());
            lastProcessedVersionstamp = versionstamp;
        }

        if (!foundEntries) {
            hasMore = false;
        } else {
            // Advance begin selector for the next batch
            if (lastProcessedVersionstamp != null) {
                Tuple nextKey;
                if (DefaultIndexDefinition.ID.selector().equals(indexDefinition.selector())) {
                    // For primary index (_id) index: [ENTRIES_MAGIC, Versionstamp]
                    nextKey = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), lastProcessedVersionstamp);
                } else {
                    // For the secondary indexes: [ENTRIES_MAGIC, indexed_value, Versionstamp]
                    Object indexValue = cursorManager.extractIndexValueFromBqlValue(lastProcessedIndexValue);
                    nextKey = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), indexValue, lastProcessedVersionstamp);
                }
                byte[] nextKeyBytes = indexSubspace.pack(nextKey);
                currentBeginSelector = KeySelector.firstGreaterThan(nextKeyBytes);
            }
        }
    }

    RoaringBitmap getAccumulatedEntryIds() {
        return accumulatedEntryIds;
    }

    Map<Versionstamp, Integer> getVersionstampToEntryIdMapping() {
        return versionstampToEntryId;
    }

    void clearAccumulated() {
        accumulatedEntryIds.clear();
        versionstampToEntryId.clear();
    }

    IndexDefinition getIndexDefinition() {
        return indexDefinition;
    }

    /**
     * Gets the last processed index value for cursor management.
     * This is the index value from the last entry processed in the most recent batch.
     */
    BqlValue getLastProcessedIndexValue() {
        return lastProcessedIndexValue;
    }

    /**
     * Gets the last processed versionstamp for cursor management.
     * This is the versionstamp from the last entry processed in the most recent batch.
     */
    Versionstamp getLastProcessedVersionstamp() {
        return lastProcessedVersionstamp;
    }
}