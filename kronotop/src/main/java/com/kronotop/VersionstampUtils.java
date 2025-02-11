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

package com.kronotop;

import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.io.BaseEncoding;

/**
 * The VersionstampUtils class provides utility methods for encoding and decoding
 * Versionstamp objects to and from a Base32Hex encoded format. This class utilizes
 * a custom padding character for Base32Hex encoding.
 * <p>
 * The "base32hex" encoding specified by <a
 * href="http://tools.ietf.org/html/rfc4648#section-7">RFC 4648 section 7</a>, Base 32 Encoding
 * with Extended Hex Alphabet. There is no corresponding encoding in RFC 3548.
 */
public class VersionstampUtils {
    /**
     * A BaseEncoding instance configured for Base32Hex encoding with a custom padding character ('x').
     * Used for encoding and decoding Versionstamp objects in a Base32Hex format.
     */
    private static final BaseEncoding BASE32_HEX = BaseEncoding.base32Hex().withPadChar('x');

    /**
     * Encodes a Versionstamp object into a Base32Hex encoded string.
     *
     * @param versionstamp The Versionstamp object to encode.
     * @return The Base32Hex encoded string representation of the Versionstamp object.
     */
    public static String base32HexEncode(Versionstamp versionstamp) {
        return BASE32_HEX.encode(versionstamp.getBytes());
    }

    /**
     * Decodes a Base32Hex encoded Versionstamp string into a Versionstamp object.
     *
     * @param versionstamp The Base32Hex encoded Versionstamp string to decode.
     * @return The decoded Versionstamp object.
     */
    public static Versionstamp base32HexDecode(String versionstamp) {
        return Versionstamp.fromBytes(BASE32_HEX.decode(versionstamp));
    }
}
