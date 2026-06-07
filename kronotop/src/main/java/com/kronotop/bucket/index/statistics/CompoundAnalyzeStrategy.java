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
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.maintenance.IndexMaintenanceRoutineException;
import org.bson.BsonBinary;
import org.bson.BsonValue;

import java.util.List;
import java.util.TreeSet;

/**
 * Analyze strategy for compound indexes. Packs multiple field values into a single byte array
 * preserving the lexicographic ordering of the FDB tuple layer.
 */
class CompoundAnalyzeStrategy implements IndexAnalyzeStrategy {
    private int fieldCount;

    @Override
    public IndexHolder<?> loadIndex(BucketMetadata metadata, long indexId) {
        CompoundIndex index = metadata.compoundIndexes().getIndexById(indexId, IndexSelectionPolicy.ALL);
        if (index == null) {
            return null;
        }
        CompoundIndex ready = metadata.compoundIndexes().getIndexById(indexId, IndexSelectionPolicy.READ);
        if (ready == null) {
            throw new IndexMaintenanceRoutineException("Index is not ready to analyze");
        }
        this.fieldCount = ready.definition().fields().size();
        return ready;
    }

    @Override
    public Object extractKey(Tuple unpacked, int startPos) {
        Object[] fields = new Object[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fields[i] = unpacked.get(startPos + i);
        }
        return Tuple.from(fields).pack();
    }

    @Override
    public void filterBsonValues(IndexHolder<?> index, List<Object> values, TreeSet<BsonValue> target) {
        for (Object value : values) {
            if (value instanceof byte[] bytes) {
                target.add(new BsonBinary(bytes));
            }
        }
    }

    @Override
    public IndexDefinition loadDefinition(ReadTransaction tr, DirectorySubspace indexSubspace) {
        return CompoundIndexUtil.loadIndexDefinition(tr, indexSubspace);
    }

    @Override
    public boolean isPrimary(IndexHolder<?> index) {
        return false;
    }
}
