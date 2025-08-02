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

import com.kronotop.Context;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BucketMetadataRegistry {
    private final Context context;
    private final Map<String, BucketMetadataWrapper> registry = new ConcurrentHashMap<>();

    public BucketMetadataRegistry(Context context) {
        this.context = context;
    }

    public void register(String bucket, BucketMetadata metadata) {
        BucketMetadataWrapper wrapper = new BucketMetadataWrapper(metadata, context.now());
        registry.put(bucket, wrapper);
    }

    public BucketMetadata getBucketMetadata(String bucket) {
        BucketMetadataWrapper wrapper = registry.get(bucket);
        if (wrapper == null) {
            return null;
        }
        wrapper.setLastAccess(context.now());
        return wrapper.getMetadata();
    }

    public boolean isEmpty() {
        return registry.isEmpty();
    }

    public Set<Map.Entry<String, BucketMetadataWrapper>> entries() {
        return registry.entrySet();
    }

    public static class BucketMetadataWrapper {
        final BucketMetadata metadata;
        long lastAccess;

        public BucketMetadataWrapper(BucketMetadata metadata, long lastAccess) {
            this.metadata = metadata;
            this.lastAccess = lastAccess;
        }

        public BucketMetadata getMetadata() {
            return metadata;
        }

        public long getLastAccess() {
            return lastAccess;
        }

        public void setLastAccess(long lastAccess) {
            this.lastAccess = lastAccess;
        }
    }
}
