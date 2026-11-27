/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.index;

import com.apple.foundationdb.tuple.Tuple;

public class SortDirectionTransform {

    public static Object apply(Object value, SortDirection direction) {
        if (direction == SortDirection.ASC) {
            return value;
        }
        // The direction is DESC, reverse the value.
        byte[] packed = Tuple.from(value).pack();
        byte[] out = new byte[packed.length + 1];
        for (int i = 0; i < packed.length; i++) {
            out[i] = (byte) (packed[i] ^ 0xFF);
        }
        out[packed.length] = (byte) 0xFF;
        return out;
    }

    public static byte[] revert(byte[] data, SortDirection direction) {
        if (direction == SortDirection.ASC) {
            return data;
        }

        // Revert the applied direction logic.
        byte[] out = new byte[data.length - 1];
        for (int i = 0; i < data.length - 1; i++) {
            out[i] = (byte) (data[i] ^ 0xFF);
        }
        return out;
    }
}
