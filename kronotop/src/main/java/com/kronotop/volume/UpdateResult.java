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

import java.util.function.BiConsumer;

public class UpdateResult {
    private final UpdatedEntry[] entries;
    private final BiConsumer<Versionstamp, EntryMetadata> cacheUpdater;

    UpdateResult(UpdatedEntry[] entries, BiConsumer<Versionstamp, EntryMetadata> cacheUpdater) {
        this.entries = entries;
        this.cacheUpdater = cacheUpdater;
    }

    public void complete() {
        // TODO: Check entry.metadata.length before update
        for (UpdatedEntry entry : entries) {
            cacheUpdater.accept(entry.versionstamp(), entry.metadata());
        }
    }

    public UpdatedEntry[] entries() {
        return entries;
    }
}
