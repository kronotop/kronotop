/*
 * Copyright (c) 2023-2025 Burak Sezer
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IndexMaintenanceTask {
    private final String namespace;
    private final String bucket;
    private final String index;
    private final long indexId;

    @JsonCreator
    public IndexMaintenanceTask(
            @JsonProperty("namespace") String namespace,
            @JsonProperty("bucket") String bucket,
            @JsonProperty("index") String index,
            @JsonProperty("indexId") long indexId) {
        this.namespace = namespace;
        this.bucket = bucket;
        this.index = index;
        this.indexId = indexId;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getBucket() {
        return bucket;
    }

    public String getIndex() {
        return index;
    }

    public long getIndexId() {
        return indexId;
    }

    @Override
    public String toString() {
        return "IndexMaintenanceTask{" +
                "namespace='" + namespace + '\'' +
                ", bucket='" + bucket + '\'' +
                ", index='" + index + '\'' +
                ", indexId=" + indexId +
                '}';
    }
}
