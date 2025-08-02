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

package com.kronotop.bucket.bql.values;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class CustomTypes {
    private static final Map<Byte, CustomType> BY_CODE = new ConcurrentHashMap<>();

    public static void register(CustomType t) {
        BY_CODE.put(t.code(), t);
    }

    public static CustomType from(byte code) {
        return BY_CODE.get(code);
    }
}