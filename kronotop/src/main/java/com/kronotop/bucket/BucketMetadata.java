/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.cache.Cache;
import com.kronotop.bucket.index.CompoundIndexRegistry;
import com.kronotop.bucket.index.SingleFieldIndexRegistry;
import com.kronotop.bucket.index.VectorIndexRegistry;
import com.kronotop.volume.Prefix;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.UUID;

public record BucketMetadata(UUID uuid,
                             String namespace,
                             String name,
                             long version,
                             boolean removed,
                             DirectorySubspace subspace,
                             DirectorySubspace pointerSubspace,
                             Prefix prefix,
                             SingleFieldIndexRegistry indexes,
                             CompoundIndexRegistry compoundIndexes,
                             VectorIndexRegistry vectorIndexes,
                             List<Integer> shards,
                             Collation collation,
                             Cache<ObjectId, Versionstamp> volumePointerCache) {
}
