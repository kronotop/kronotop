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

package com.kronotop.ctl;

import com.kronotop.resp.RespValue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

// Builds the server replies that the tests feed to the parsers.
final class RespValues {

    private RespValues() {
    }

    static RespValue text(String value) {
        return new RespValue.BlobString(value);
    }

    static RespValue texts(String... values) {
        return new RespValue.Array(Stream.of(values).map(RespValues::text).toList());
    }

    // Builds a map reply from alternating keys and values, in the given order.
    static RespValue map(RespValue... pairs) {
        Map<RespValue, RespValue> values = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            values.put(pairs[i], pairs[i + 1]);
        }
        return new RespValue.RespMap(values);
    }
}
