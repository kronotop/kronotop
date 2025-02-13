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

public class DeleteResult {
    private final Versionstamp[] keys;
    private final Consumer<Versionstamp> cacheUpdater;

    DeleteResult(int capacity, Consumer<Versionstamp> cacheUpdater) {
        this.keys = new Versionstamp[capacity];
        this.cacheUpdater = cacheUpdater;
    }

    protected void add(int index, Versionstamp key) {
        keys[index] = key;
    }

    public void complete() {
        for (Versionstamp key : keys) {
            if (key == null) {
                continue;
            }
            cacheUpdater.accept(key);
        }
    }
}
