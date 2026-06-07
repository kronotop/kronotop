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

package com.kronotop.zmap;

import com.kronotop.KronotopException;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Encodes and decodes typed values for ZMap typed counters (I64, F64, D128).
 * All encodings use little-endian byte order to match FoundationDB atomic mutation conventions.
 */
public final class ZMapNumericValueCodec {

    private ZMapNumericValueCodec() {
    }

    /**
     * Encodes a 64-bit integer as an 8-byte little-endian two's-complement array.
     *
     * @param value the long value to encode
     * @return 8-byte little-endian representation
     */
    public static byte[] encodeI64(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }

    /**
     * Decodes an 8-byte little-endian array into a 64-bit integer.
     *
     * @param raw the byte array to decode (must be exactly 8 bytes)
     * @return the decoded long value
     * @throws KronotopException if the array length is not 8
     */
    public static long decodeI64(byte[] raw) {
        if (raw.length != 8) {
            throw new KronotopException("Invalid stored value: expected 8-byte two's-complement int64");
        }
        return ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    /**
     * Throws if the given double is not finite (NaN or Infinity).
     *
     * @param value   the double to validate
     * @param message the error message to use if validation fails
     * @throws KronotopException if the value is NaN or infinite
     */
    public static void validateFiniteF64(double value, String message) {
        if (!Double.isFinite(value)) {
            throw new KronotopException(message);
        }
    }

    /**
     * Encodes a double as an 8-byte little-endian IEEE-754 array. Normalizes {@code -0.0} to {@code 0.0}.
     *
     * @param value the double value to encode
     * @return 8-byte little-endian representation
     */
    public static byte[] encodeF64(double value) {
        double normalized = (value == -0.0d) ? 0.0d : value;
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(normalized).array();
    }

    /**
     * Decodes an 8-byte little-endian array into an IEEE-754 double.
     *
     * @param raw the byte array to decode (must be exactly 8 bytes)
     * @return the decoded double value
     * @throws KronotopException if the array length is not 8
     */
    public static double decodeF64(byte[] raw) {
        if (raw.length != 8) {
            throw new KronotopException("Invalid stored value: expected 8-byte IEEE-754 double");
        }
        return ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN).getDouble();
    }

    /**
     * Encodes a Decimal128 as a 16-byte little-endian IEEE-754 BID array (low word first, then high word).
     *
     * @param value the Decimal128 value to encode
     * @return 16-byte little-endian representation
     */
    public static byte[] encodeD128(Decimal128 value) {
        ByteBuffer w = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        w.putLong(value.getLow());
        w.putLong(value.getHigh());
        return w.array();
    }

    /**
     * Decodes a 16-byte little-endian IEEE-754 BID array into a Decimal128.
     *
     * @param raw the byte array to decode (must be exactly 16 bytes)
     * @return the decoded Decimal128 value
     * @throws KronotopException if the array length is not 16
     */
    public static Decimal128 decodeD128(byte[] raw) {
        if (raw.length != 16) {
            throw new KronotopException("Invalid stored value: expected 16-byte Decimal128 (IEEE-754 BID)");
        }
        ByteBuffer r = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
        long low = r.getLong();
        long high = r.getLong();
        return Decimal128.fromIEEE754BIDEncoding(high, low);
    }

    /**
     * Parses a decimal string into a Decimal128 via BigDecimal, validating range.
     *
     * @param value the decimal string to parse
     * @return the parsed Decimal128 value
     * @throws KronotopException if the string is not a valid decimal or exceeds Decimal128 range
     */
    public static Decimal128 parseDecimal128(String value) {
        BigDecimal bd;
        try {
            bd = new BigDecimal(value);
        } catch (NumberFormatException e) {
            throw new KronotopException("invalid decimal");
        }
        try {
            return new Decimal128(bd);
        } catch (NumberFormatException e) {
            throw new KronotopException(e.getMessage(), e);
        }
    }
}
