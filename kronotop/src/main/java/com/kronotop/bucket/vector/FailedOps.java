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

package com.kronotop.bucket.vector;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.volume.EntryMetadata;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * Adds and deletes that failed to apply to the on-heap vector index during a log replay.
 */
public record FailedOps(List<Add> adds, List<Delete> deletes) {

    /**
     * An insert or update that failed to apply. Holds everything needed to retry the add later.
     */
    public record Add(
            Versionstamp versionstamp,
            ObjectId objectId,
            int shardId,
            EntryMetadata metadata,
            float[] vector
    ) {
    }

    /**
     * A delete that failed to apply. Holds the ObjectId and the versionstamp of the log entry.
     */
    public record Delete(ObjectId objectId, Versionstamp versionstamp) {
    }
}
