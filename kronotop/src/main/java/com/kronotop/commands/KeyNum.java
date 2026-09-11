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
package com.kronotop.commands;

/**
 * Keynum based find_keys spec. The argument at keynumidx holds the number of keys.
 */
public record KeyNum(int keynumidx, int firstkey, int step) {
    public KeyNum {
        if (step < 1) {
            throw new IllegalArgumentException("key spec keynum step must be >= 1");
        }
        if (keynumidx < 0 || firstkey < 0) {
            throw new IllegalArgumentException("key spec keynum keynumidx and firstkey must be >= 0");
        }
    }
}
