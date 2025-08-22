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

package com.kronotop.bucket.bql.ast;

import java.util.Arrays;
import java.util.Base64;

public record BinaryVal(byte[] value) implements BqlValue {
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof BinaryVal(byte[] binaryValue))) return false;
        return Arrays.equals(value, binaryValue);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    public String toJson() {
        return "\"" + Base64.getEncoder().encodeToString(value) + "\"";
    }
}
