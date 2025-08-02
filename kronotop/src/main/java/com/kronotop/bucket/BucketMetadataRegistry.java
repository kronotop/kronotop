// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

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
