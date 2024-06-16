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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class AppendResult {
    private final CompletableFuture<byte[]> future;
    private final List<EntryMetadata> entryMetadataList;
    private final BiConsumer<Versionstamp, EntryMetadata> cacheUpdater;
    private boolean calledOnce = false;

    AppendResult(CompletableFuture<byte[]> future, List<EntryMetadata> entryMetadataList, BiConsumer<Versionstamp, EntryMetadata> cacheUpdater) {
        this.future = future;
        this.entryMetadataList = entryMetadataList;
        this.cacheUpdater = cacheUpdater;
    }

    public synchronized List<Versionstamp> getVersionstampedKeys() {
        if (calledOnce) {
            throw new IllegalStateException("this method was called before once");
        }
        byte[] trVersion = future.join();
        calledOnce = true;
        int userVersion = 0;
        List<Versionstamp> versionstampedKeys = new ArrayList<>();
        for (EntryMetadata entryMetadata : entryMetadataList) {
            Versionstamp versionstampedKey = Versionstamp.complete(trVersion, userVersion);
            cacheUpdater.accept(versionstampedKey, entryMetadata);
            versionstampedKeys.add(versionstampedKey);
            userVersion++;
        }
        return versionstampedKeys;
    }
}
