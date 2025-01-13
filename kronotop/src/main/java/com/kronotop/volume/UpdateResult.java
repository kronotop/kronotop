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

import java.util.function.Consumer;

public class UpdateResult {
    private final KeyEntry[] pairs;
    private final Consumer<Versionstamp> cacheUpdater;

    UpdateResult(KeyEntry[] pairs, Consumer<Versionstamp> cacheUpdater) {
        this.pairs = pairs;
        this.cacheUpdater = cacheUpdater;
    }

    public void complete() {
        for (KeyEntry pair : pairs) {
            cacheUpdater.accept(pair.key());
        }
    }
}
