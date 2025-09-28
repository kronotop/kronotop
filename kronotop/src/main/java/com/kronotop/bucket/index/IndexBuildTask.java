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

import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.kronotop.internal.VersionstampDeserializer;
import com.kronotop.internal.VersionstampSerializer;

public class IndexBuildTask extends IndexTask {
    private final String namespace;
    private final String bucket;
    private final long indexId;
    private boolean completed;
    private String error;

    @JsonSerialize(using = VersionstampSerializer.class)
    @JsonDeserialize(using = VersionstampDeserializer.class)
    private Versionstamp highestVersionstamp;

    @JsonSerialize(using = VersionstampSerializer.class)
    @JsonDeserialize(using = VersionstampDeserializer.class)
    private Versionstamp cursorVersionstamp;


    @JsonCreator
    public IndexBuildTask(
            @JsonProperty("namespace") String namespace,
            @JsonProperty("bucket") String bucket,
            @JsonProperty("indexId") long indexId) {
        super(IndexTaskKind.BUILD);
        this.namespace = namespace;
        this.bucket = bucket;
        this.indexId = indexId;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getBucket() {
        return bucket;
    }

    public long getIndexId() {
        return indexId;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public Versionstamp getHighestVersionstamp() {
        return highestVersionstamp;
    }

    public void setHighestVersionstamp(Versionstamp highestVersionstamp) {
        this.highestVersionstamp = highestVersionstamp;
    }

    public void setCursorVersionstamp(Versionstamp cursorVersionstamp) {
        this.cursorVersionstamp = cursorVersionstamp;
    }

    public Versionstamp getCursorVersionstamp() {
        return cursorVersionstamp;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getError() {
        return error;
    }
}
