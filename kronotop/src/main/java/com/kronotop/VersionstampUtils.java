/*
 * Copyright (c) 2023-2025 Kronotop
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

package com.kronotop;

import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.io.BaseEncoding;

/**
 * Utility class for encoding and decoding Versionstamps using Base64 encoding.
 */
public class VersionstampUtils {

    /**
     * Encodes a Versionstamp object using Base64 encoding.
     *
     * @param versionstamp The Versionstamp object to encode.
     * @return The Base64 encoded string representation of the Versionstamp object.
     */
    public static String base64Encode(Versionstamp versionstamp) {
        return BaseEncoding.base64().encode(versionstamp.getBytes());
    }

    /**
     * Decodes a Base64 encoded Versionstamp string and returns the corresponding Versionstamp object.
     *
     * @param versionstamp The Base64 encoded Versionstamp string to decode.
     * @return The decoded Versionstamp object.
     */
    public static Versionstamp base64Decode(String versionstamp) {
        return Versionstamp.fromBytes(BaseEncoding.base64().decode(versionstamp));
    }
}
