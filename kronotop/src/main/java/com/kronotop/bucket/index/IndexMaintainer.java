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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.CollatorCache;

// IndexSubspaceMagic.WATERMARK and IndexSubspaceMagic.VOLUME_POINTER exist because there is no way to maintain a single
// watermark key for tracking the latest versionstamp written to an index. With many JVMs writing concurrently,
// SET_VERSIONSTAMPED_VALUE would cause transaction conflicts, and BYTE_MAX (conflict-free)
// does not support commit-time versionstamp injection. FoundationDB cannot combine these two
// primitives, so each insert must write its own versionstamped key. The background index builder
// finds the last written entry via a reverse range read on VOLUME_POINTER.

public class IndexMaintainer {
    /**
     * Placeholder value for back pointer entries.
     */
    protected final static byte[] NULL_VALUE = new byte[]{0};

    public static Collation resolveCollation(IndexDefinition definition, BucketMetadata metadata) {
        Collation c = definition.collation();
        return c != null ? c : metadata.collation();
    }

    public static Object applyCollation(Object value, Collation collation, CollatorCache cache) {
        if (value instanceof String str && collation != null) {
            return cache.acquire(collation).getCollationKey(str).toByteArray();
        }
        return value;
    }

    /**
     * Appends a versionstamped watermark key to track index write progress.
     *
     * <p>Each insert writes its own key because FDB cannot combine conflict-free mutations
     * with commit-time versionstamp injection. The background index builder finds the latest
     * watermark via a reverse range read to determine where to resume.
     *
     * <p>Uses {@code SET_VERSIONSTAMPED_KEY} which is conflict-free on the write path.
     *
     * @param tr            the FoundationDB transaction
     * @param indexSubspace the index's directory subspace
     * @param userVersion   the user version for ordering within a single transaction
     */
    protected static void addWatermark(Transaction tr, DirectorySubspace indexSubspace, int userVersion) {
        // Watermark: (WATERMARK, Versionstamp)
        Tuple watermarkTuple = Tuple.from(
                IndexSubspaceMagic.WATERMARK.getValue(),
                Versionstamp.incomplete(userVersion)
        );
        byte[] watermarkKey = indexSubspace.packWithVersionstamp(watermarkTuple);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, watermarkKey, NULL_VALUE);
    }

    protected static void addWatermark(Transaction tr, DirectorySubspace indexSubspace, Versionstamp versionstamp) {
        // Watermark: (WATERMARK, Versionstamp)
        Tuple watermarkTuple = Tuple.from(
                IndexSubspaceMagic.WATERMARK.getValue(),
                versionstamp
        );
        byte[] watermarkKey = indexSubspace.pack(watermarkTuple);
        tr.set(watermarkKey, NULL_VALUE);
    }

    /**
     * Clears all watermark keys for an index.
     *
     * @param tr            the FoundationDB transaction
     * @param indexSubspace the index's directory subspace
     */
    public static void cleanWatermark(Transaction tr, DirectorySubspace indexSubspace) {
        // Watermark: (WATERMARK, Versionstamp)
        Tuple watermarkTuple = Tuple.from(
                IndexSubspaceMagic.WATERMARK.getValue()
        );
        byte[] begin = indexSubspace.pack(watermarkTuple);
        tr.clear(Range.startsWith(begin));
    }
}
