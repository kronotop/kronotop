/*
 * Copyright (c) 2023-2024 Kronotop
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
import com.google.common.cache.LoadingCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class AppendResult {
    private final CompletableFuture<byte[]> future;
    private final List<EntryMetadata> entryMetadataList;
    private final LoadingCache<Versionstamp, EntryMetadata> cache;

    AppendResult(CompletableFuture<byte[]> future, List<EntryMetadata> entryMetadataList, LoadingCache<Versionstamp, EntryMetadata> cache) {
        this.future = future;
        this.entryMetadataList = entryMetadataList;
        this.cache = cache;
    }

    private void cacheEntryMetadata(List<Versionstamp> versionstampList, List<EntryMetadata> entryMetadataList) {
        for (int i = 0; i < versionstampList.size(); i++) {
            Versionstamp key = versionstampList.get(i);
            EntryMetadata value = entryMetadataList.get(i);
            cache.put(key, value);
        }
    }

    public List<Versionstamp> getVersionstampedKeys() {
        byte[] trVersion = future.join();
        List<Versionstamp> versionstampedKeys = new ArrayList<>();
        for (int i = 0; i < entryMetadataList.size(); i++) {
            versionstampedKeys.add(Versionstamp.complete(trVersion, i));
        }
        cacheEntryMetadata(versionstampedKeys, entryMetadataList);
        return versionstampedKeys;
    }
}
