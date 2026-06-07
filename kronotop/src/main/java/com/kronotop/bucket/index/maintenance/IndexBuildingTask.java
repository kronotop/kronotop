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

package com.kronotop.bucket.index.maintenance;

import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kronotop.bucket.index.IndexType;
import com.kronotop.internal.VersionstampDeserializer;
import com.kronotop.internal.VersionstampSerializer;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;

public class IndexBuildingTask extends IndexMaintenanceTask {
    @JsonSerialize(using = VersionstampSerializer.class)
    @JsonDeserialize(using = VersionstampDeserializer.class)
    private final Versionstamp lower;

    @JsonSerialize(using = VersionstampSerializer.class)
    @JsonDeserialize(using = VersionstampDeserializer.class)
    private final Versionstamp upper;

    private final IndexType indexType;

    @JsonCreator
    public IndexBuildingTask(
            @JsonProperty("namespace") String namespace,
            @JsonProperty("bucket") String bucket,
            @JsonProperty("indexId") long indexId,
            @JsonProperty("shardId") int shardId,
            @JsonProperty("lower") Versionstamp lower,
            @JsonProperty("upper") Versionstamp upper,
            @JsonProperty("indexType") IndexType indexType) {
        super(IndexMaintenanceTaskKind.BUILD, namespace, bucket, indexId, shardId);
        this.lower = lower;
        this.upper = upper;
        this.indexType = indexType;
    }

    public IndexBuildingTask(String namespace, String bucket, long indexId, int shardId,
                             Versionstamp lower, Versionstamp upper) {
        this(namespace, bucket, indexId, shardId, lower, upper, IndexType.SINGLE_FIELD);
    }

    public Versionstamp getLower() {
        return lower;
    }

    public Versionstamp getUpper() {
        return upper;
    }

    public IndexType getIndexType() {
        return indexType;
    }
}
