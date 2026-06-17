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

import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.CollatorCache;

// The background index builder determines its scan boundaries from the primary index VOLUME_POINTER
// subspace: the first entry is the inclusive lower bound and the last entry (reverse range read) gives
// the exclusive upper bound. The build always covers the full range and re-sets entries idempotently.

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
}
