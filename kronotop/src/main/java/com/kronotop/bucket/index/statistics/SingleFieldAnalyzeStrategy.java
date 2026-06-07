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
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.maintenance.IndexMaintenanceRoutineException;
import org.bson.BsonObjectId;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.TreeSet;

/**
 * Analyze strategy for single-field indexes.
 */
class SingleFieldAnalyzeStrategy implements IndexAnalyzeStrategy {

    @Override
    public IndexHolder<?> loadIndex(BucketMetadata metadata, long indexId) {
        Index index = metadata.indexes().getIndexById(indexId, IndexSelectionPolicy.ALL);
        if (index == null) {
            return null;
        }
        Index ready = metadata.indexes().getIndexById(indexId, IndexSelectionPolicy.READ);
        if (ready == null) {
            throw new IndexMaintenanceRoutineException("Index is not ready to analyze");
        }
        return ready;
    }

    @Override
    public Object extractKey(Tuple unpacked, int startPos) {
        return unpacked.get(startPos);
    }

    @Override
    public void filterBsonValues(IndexHolder<?> index, List<Object> values, TreeSet<BsonValue> target) {
        SingleFieldIndexDefinition def = (SingleFieldIndexDefinition) index.definition();
        for (Object value : values) {
            BsonValue bsonValue;
            if (def.bsonType() == BsonType.OBJECT_ID && value instanceof byte[] bytes) {
                bsonValue = new BsonObjectId(new ObjectId(bytes));
            } else {
                // FDB tuple layer always returns Long for integers.
                // Narrow back to Integer when the index declares INT32.
                if (value instanceof Long longVal && def.bsonType() == BsonType.INT32) {
                    value = longVal.intValue();
                }
                bsonValue = BSONUtil.toBsonValue(value);
            }
            if (BSONUtil.equals(bsonValue.getBsonType(), def.bsonType())) {
                target.add(bsonValue);
            }
        }
    }

    @Override
    public IndexDefinition loadDefinition(ReadTransaction tr, DirectorySubspace indexSubspace) {
        return SingleFieldIndexUtil.loadIndexDefinition(tr, indexSubspace);
    }

    @Override
    public boolean isPrimary(IndexHolder<?> index) {
        return PrimaryIndex.isPrimary((SingleFieldIndexDefinition) index.definition());
    }
}
