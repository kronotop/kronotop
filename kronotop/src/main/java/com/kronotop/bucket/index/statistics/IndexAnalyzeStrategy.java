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

package com.kronotop.bucket.index.statistics;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexHolder;
import org.bson.BsonValue;

import java.util.List;
import java.util.TreeSet;

/**
 * Encapsulates index-type-specific behavior for the analyze routine.
 */
interface IndexAnalyzeStrategy {

    /**
     * Loads and validates the index from bucket metadata.
     *
     * @return the index holder, or null if the index does not exist in this registry
     */
    IndexHolder<?> loadIndex(BucketMetadata metadata, long indexId);

    /**
     * Extracts the indexed key from an unpacked FDB tuple starting at the given position.
     */
    Object extractKey(Tuple unpacked, int startPos);

    /**
     * Converts raw extracted values into BsonValues and adds them to the target set.
     */
    void filterBsonValues(IndexHolder<?> index, List<Object> values, TreeSet<BsonValue> target);

    /**
     * Loads the current index definition from FoundationDB for validation.
     */
    IndexDefinition loadDefinition(ReadTransaction tr, DirectorySubspace indexSubspace);

    /**
     * Returns true if this index is a primary index.
     */
    boolean isPrimary(IndexHolder<?> index);
}
