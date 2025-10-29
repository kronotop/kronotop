/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.index.statistics;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.bson.BsonValue;

/**
 * Probabilistic selector for index statistics sampling.
 * Uses MurmurHash3 for deterministic, hot-path friendly selection.
 */
public class ProbabilisticSelector {
    private static final HashFunction MURMUR3_32_FIXED = Hashing.murmur3_32_fixed();

    // 14-bit mask: probability of 1/16,384 (~1/10,000 target)
    // Using (hash & MASK) == 0 gives us approximately 1 in 16,384 selection rate
    private static final int MASK = 0x3FFF;

    /**
     * Determines if the given value should be selected for sampling.
     * Uses deterministic hashing with ~1/16,384 probability.
     *
     * @param value the BSON value to evaluate
     * @return true if selected, false otherwise
     */
    public static boolean match(BsonValue value) {
        // Fast probabilistic filter using MurmurHash3 (non-cryptographic)
        // Deterministic for same input, minimal allocations for hot-path performance
        int hash = computeHash(value);
        return (hash & MASK) == 0;
    }

    /**
     * Determines whether the given 64-bit integer value should be selected
     * for sampling based on a probabilistic hash.
     *
     * @param value the 64-bit integer value to evaluate
     * @return true if the value is selected based on a ~1/16,384 probability, false otherwise
     */
    public static boolean match(long value) {
        int hash = (int) (value ^ (value >>> 32));
        return (hash & MASK) == 0;
    }

    /**
     * Computes hash for BsonValue with minimal allocations.
     * Uses numeric values directly, hashes binary/string types.
     */
    private static int computeHash(BsonValue value) {
        if (value == null) {
            return 0;
        }
        return switch (value.getBsonType()) {
            case INT32 -> value.asInt32().getValue();
            case INT64 -> {
                long v = value.asInt64().getValue();
                yield (int) (v ^ (v >>> 32));
            }
            case DOUBLE -> {
                long bits = Double.doubleToLongBits(value.asDouble().getValue());
                yield (int) (bits ^ (bits >>> 32));
            }
            case BOOLEAN -> value.asBoolean().getValue() ? 1 : 0;
            case DATE_TIME -> {
                long millis = value.asDateTime().getValue();
                yield (int) (millis ^ (millis >>> 32));
            }
            case TIMESTAMP -> {
                long ts = value.asTimestamp().getValue();
                yield (int) (ts ^ (ts >>> 32));
            }
            case DECIMAL128 -> value.asDecimal128().getValue().hashCode();
            case STRING -> MURMUR3_32_FIXED
                    .hashUnencodedChars(value.asString().getValue())
                    .asInt();
            case BINARY -> MURMUR3_32_FIXED
                    .hashBytes(value.asBinary().getData())
                    .asInt();
            case NULL -> 0;
            default -> throw new IllegalArgumentException("Unknown BsonType: " + value.getBsonType());
        };
    }
}
