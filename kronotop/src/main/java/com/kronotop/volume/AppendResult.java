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

package com.kronotop.volume;

import com.apple.foundationdb.tuple.Versionstamp;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class AppendResult {
    private final CompletableFuture<byte[]> future;
    private final EntryMetadata[] entryMetadataList;
    private final BiConsumer<Versionstamp, EntryMetadata> cacheUpdater;
    private boolean calledOnce = false;

    AppendResult(CompletableFuture<byte[]> future, EntryMetadata[] entryMetadataList, BiConsumer<Versionstamp, EntryMetadata> cacheUpdater) {
        this.future = future;
        this.entryMetadataList = entryMetadataList;
        this.cacheUpdater = cacheUpdater;
    }

    public synchronized Versionstamp[] getVersionstampedKeys() {
        if (calledOnce) {
            throw new IllegalStateException("this method was called before once");
        }
        byte[] trVersion = future.join();
        calledOnce = true;
        int userVersion = 0;
        Versionstamp[] versionstampedKeys = new Versionstamp[entryMetadataList.length];
        for (EntryMetadata entryMetadata : entryMetadataList) {
            Versionstamp versionstampedKey = Versionstamp.complete(trVersion, userVersion);
            cacheUpdater.accept(versionstampedKey, entryMetadata);
            versionstampedKeys[userVersion] = versionstampedKey;
            userVersion++;
        }
        return versionstampedKeys;
    }

    /**
     * Updates the entry metadata cache by invoking the {@code getVersionstampedKeys} method.
     * <p>
     * This method ensures that the metadata cache is refreshed by processing all {@code EntryMetadata}
     * objects with their corresponding version-stamped keys. The version-stamped keys are generated
     * and linked with the provided callback by leveraging the implementation of {@code getVersionstampedKeys}.
     * <p>
     * Note:
     * - The {@code getVersionstampedKeys} method is synchronized and can only be called once. Calling it more than once
     * will result in an {@code IllegalStateException}.
     * - The internal processing relies on the completion of the associated {@code future}.
     * <p>
     * This method is typically used to synchronize the latest version-stamped keys with the provided cache
     * update mechanism.
     */
    public void updateEntryMetadataCache() {
        getVersionstampedKeys();
    }

    public EntryMetadata[] getEntryMetadataList() {
        return entryMetadataList;
    }
}
