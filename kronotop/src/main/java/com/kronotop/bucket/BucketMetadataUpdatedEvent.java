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

package com.kronotop.bucket;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kronotop.cluster.BaseBroadcastEvent;
import com.kronotop.cluster.BroadcastEventKind;

@JsonIgnoreProperties(ignoreUnknown = true)
class BucketMetadataUpdatedEvent extends BaseBroadcastEvent {
    private String namespace;
    private String bucket;
    private long id;
    private long minimumVersion;

    BucketMetadataUpdatedEvent() {
    }

    public BucketMetadataUpdatedEvent(String namespace, String bucket, long id, long minimumVersion) {
        super(BroadcastEventKind.BUCKET_METADATA_UPDATED_EVENT);
        this.namespace = namespace;
        this.bucket = bucket;
        this.id = id;
        this.minimumVersion = minimumVersion;
    }

    @JsonProperty
    public String namespace() {
        return namespace;
    }

    @JsonProperty
    public String bucket() {
        return bucket;
    }

    @JsonProperty
    public long id() {
        return id;
    }

    @JsonProperty
    public long minimumVersion() {
        return minimumVersion;
    }

    @Override
    public String toString() {
        return String.format(
                "BucketMetadataUpdatedEvent {namespace=%s, bucket=%s, id=%d, minimumVersion=%d}",
                namespace,
                bucket,
                id,
                minimumVersion
        );
    }
}
