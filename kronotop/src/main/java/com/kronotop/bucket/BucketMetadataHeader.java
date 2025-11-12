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

package com.kronotop.bucket;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.index.IndexStatistics;
import com.kronotop.bucket.index.statistics.Histogram;
import com.kronotop.bucket.index.statistics.HistogramCodec;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the header section of bucket metadata stored in FoundationDB.
 * <p>
 * The header contains core bucket identification, versioning information, and per-index statistics.
 * This record is reconstructed by reading and parsing structured key-value pairs from FoundationDB
 * under the HEADER magic prefix.
 *
 * @param id              Unique bucket identifier generated during bucket creation
 * @param version         Bucket metadata version timestamp for optimistic concurrency control
 * @param indexStatistics Map of index ID to statistics (cardinality and histogram) for query optimization
 */
public record BucketMetadataHeader(long id, long version, Map<Long, IndexStatistics> indexStatistics) {

    /**
     * Reads and reconstructs bucket metadata header from FoundationDB.
     * <p>
     * Performs a range scan over the HEADER prefix to collect bucket ID, version, and index statistics.
     * Index statistics are assembled by grouping consecutive CARDINALITY and HISTOGRAM entries by index ID.
     *
     * @param tr       Active FoundationDB transaction for reading metadata
     * @param subspace Directory subspace containing the bucket metadata
     * @return Reconstructed header containing bucket ID, version, and all index statistics
     */
    public static BucketMetadataHeader read(Transaction tr, DirectorySubspace subspace) {
        Tuple tuple = Tuple.from(BucketMetadataMagic.HEADER.getValue());
        byte[] prefix = subspace.pack(tuple);

        long bucketId = 0;
        long bucketMetadataVersion = 0;
        HashMap<Long, IndexStatistics> stats = new HashMap<>();

        Long currentIndexId = null;
        long cardinality = 0L;
        Histogram histogram = Histogram.create();

        KeySelector begin = KeySelector.firstGreaterThan(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
        for (KeyValue entry : tr.snapshot().getRange(begin, end)) {
            Tuple unpackedKey = subspace.unpack(entry.getKey());
            if (unpackedKey.getLong(1) == BucketMetadataMagic.ID.getLong()) {
                bucketId = ByteBuffer.wrap(entry.getValue()).order(ByteOrder.LITTLE_ENDIAN).getLong();
            } else if (unpackedKey.getLong(1) == BucketMetadataMagic.VERSION.getLong()) {
                bucketMetadataVersion = ByteBuffer.wrap(entry.getValue()).order(ByteOrder.LITTLE_ENDIAN).getLong();
            } else if (unpackedKey.getLong(1) == BucketMetadataMagic.INDEX_STATISTICS.getLong()) {
                long indexId = unpackedKey.getLong(2);
                if (currentIndexId == null) {
                    // fresh start
                    currentIndexId = indexId;
                } else if (currentIndexId != indexId) {
                    // finalize the currentIndexId
                    stats.put(currentIndexId, new IndexStatistics(cardinality, histogram));
                    currentIndexId = indexId;
                    cardinality = 0;
                    histogram = Histogram.create();
                }

                long magic = unpackedKey.getLong(3);
                if (magic == BucketMetadataMagic.CARDINALITY.getLong()) {
                    cardinality = ByteBuffer.wrap(entry.getValue()).order(ByteOrder.LITTLE_ENDIAN).getLong();
                } else if (magic == BucketMetadataMagic.HISTOGRAM.getLong()) {
                    histogram = HistogramCodec.decode(entry.getValue());
                }
            }
        }

        if (currentIndexId != null) {
            // Set the final entry
            stats.put(currentIndexId, new IndexStatistics(cardinality, histogram));
        }
        return new BucketMetadataHeader(bucketId, bucketMetadataVersion, stats);
    }
}
