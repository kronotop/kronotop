/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.bucket;

import com.kronotop.bucket.executor.Cursor;

public class QueryExecutorConfig {
    // Immutable fields
    private final BucketMetadata metadata;
    private final String query;
    // Cursor state for pagination
    private final Cursor cursor = new Cursor();
    // Mutable fields
    private volatile int shardId;
    private volatile int limit;
    private volatile boolean reverse;
    private volatile long readVersion;
    private volatile boolean pinReadVersion;

    public QueryExecutorConfig(BucketMetadata metadata, String query) {
        this.metadata = metadata;
        this.query = query;
    }

    public BucketMetadata getMetadata() {
        return metadata;
    }

    public String getQuery() {
        return query;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public boolean isReverse() {
        return reverse;
    }

    public void setReverse(boolean reverse) {
        this.reverse = reverse;
    }

    public int getShardId() {
        return shardId;
    }

    public void setShardId(int shardId) {
        this.shardId = shardId;
    }

    public long getReadVersion() {
        return readVersion;
    }

    public void setReadVersion(long readVersion) {
        this.readVersion = readVersion;
    }

    public boolean getPinReadVersion() {
        return pinReadVersion;
    }

    public void setPinReadVersion(boolean pinReadVersion) {
        this.pinReadVersion = pinReadVersion;
    }

    public Cursor getCursor() {
        return cursor;
    }
}
