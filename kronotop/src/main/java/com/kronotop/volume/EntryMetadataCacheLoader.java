/*
 * Copyright (c) 2023-2025 Kronotop
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

package com.kronotop.volume;

import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.cache.CacheLoader;
import com.kronotop.Context;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.time.Duration;

class EntryMetadataCacheLoader extends CacheLoader<Versionstamp, EntryMetadata> {
    public static Duration EXPIRE_AFTER_ACCESS = Duration.ofMinutes(15);
    private final Context context;
    private final VolumeSubspace subspace;
    private final Prefix prefix;

    EntryMetadataCacheLoader(Context context, VolumeSubspace subspace, Prefix prefix) {
        this.context = context;
        this.subspace = subspace;
        this.prefix = prefix;
    }

    @Override
    public @Nonnull EntryMetadata load(@Nonnull Versionstamp key) {
        // See https://github.com/google/guava/wiki/CachesExplained#when-does-cleanup-happen
        return context.getFoundationDB().run(tr -> {
            byte[] value = tr.get(subspace.packEntryKey(prefix, key)).join();
            if (value == null) {
                return null;
            }
            return EntryMetadata.decode(ByteBuffer.wrap(value));
        });
    }
}