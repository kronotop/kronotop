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

package com.kronotop.bucket.statistics.prefix;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.internal.JSONUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Utility class for encoding/decoding Fixed N-Byte Prefix Histogram keys and values.
 * Key Schema: HIST/<idx>/N/<N>/T/<pN> = <i64>
 * No sharding - simple atomic counters.
 */
public class PrefixHistogramKeySchema {
    // Key constants
    public static final String HIST_PREFIX = "HIST";
    public static final String N_PREFIX = "N";
    public static final String T_PREFIX = "T";
    public static final String METADATA_KEY = "meta";
    
    // Little-endian encoded values for atomic ADD operations
    public static final byte[] ONE_LE = encodeCounterValue(1L);
    public static final byte[] NEGATIVE_ONE_LE = encodeCounterValue(-1L);

    /**
     * Encodes a long value in little-endian format for FoundationDB counter mutations
     */
    public static byte[] encodeCounterValue(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }

    /**
     * Decodes a little-endian long value from FoundationDB
     */
    public static long decodeCounterValue(byte[] data) {
        if (data == null || data.length != 8) {
            return 0;
        }
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    /**
     * Encodes prefix histogram metadata as JSON
     */
    public static byte[] encodeMetadata(PrefixHistogramMetadata metadata) {
        return JSONUtil.writeValueAsBytes(metadata);
    }

    /**
     * Decodes prefix histogram metadata from JSON
     */
    public static PrefixHistogramMetadata decodeMetadata(byte[] data) {
        if (data == null) {
            return null;
        }
        return JSONUtil.readValue(data, PrefixHistogramMetadata.class);
    }

    /**
     * Creates key for bucket counter: T/<pN>
     * Key Schema: HIST/<idx>/N/<N>/T/<pN> = <i64>
     */
    public static byte[] bucketCountKey(DirectorySubspace subspace, byte[] pN) {
        return subspace.pack(Tuple.from(T_PREFIX, pN));
    }

    /**
     * Creates key for metadata storage
     */
    public static byte[] metadataKey(Subspace subspace) {
        return subspace.pack(Tuple.from(METADATA_KEY));
    }

    /**
     * Creates range begin key for all bucket counters
     */
    public static byte[] bucketCountRangeBegin(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(T_PREFIX));
    }

    /**
     * Creates range end key for all bucket counters (exclusive)
     */
    public static byte[] bucketCountRangeEnd(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(T_PREFIX + "\u0000"));
    }

    /**
     * Creates range begin key for bucket counters starting from pN
     */
    public static byte[] bucketCountRangeBegin(DirectorySubspace subspace, byte[] pN) {
        return subspace.pack(Tuple.from(T_PREFIX, pN));
    }

    /**
     * Creates range end key for bucket counters ending at pN (exclusive)
     */
    public static byte[] bucketCountRangeEnd(DirectorySubspace subspace, byte[] pN) {
        return subspace.pack(Tuple.from(T_PREFIX, nextFixedN(pN)));
    }

    /**
     * Increments an N-byte array in big-endian order (lexicographically next value)
     */
    public static byte[] nextFixedN(byte[] pN) {
        byte[] result = pN.clone();
        for (int i = result.length - 1; i >= 0; i--) {
            if ((result[i] & 0xFF) < 255) {
                result[i]++;
                break;
            } else {
                result[i] = 0;
                if (i == 0) {
                    // Overflow - return original + 1 byte of zeros
                    byte[] overflow = new byte[result.length + 1];
                    System.arraycopy(result, 0, overflow, 0, result.length);
                    overflow[0] = 1;
                    return overflow;
                }
            }
        }
        return result;
    }

    /**
     * Decrements an N-byte array in big-endian order (lexicographically previous value)
     */
    public static byte[] prevFixedN(byte[] pN) {
        byte[] result = pN.clone();
        for (int i = result.length - 1; i >= 0; i--) {
            if ((result[i] & 0xFF) > 0) {
                result[i]--;
                break;
            } else {
                result[i] = (byte) 0xFF;
                if (i == 0) {
                    // Underflow - return smaller array
                    if (result.length > 1) {
                        byte[] underflow = new byte[result.length - 1];
                        System.arraycopy(result, 1, underflow, 0, underflow.length);
                        return underflow;
                    } else {
                        return new byte[0];
                    }
                }
            }
        }
        return result;
    }
}