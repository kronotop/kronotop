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

import java.nio.ByteBuffer;

/**
 * Encodes and decodes histograms for index metadata storage.
 * Uses CRLF delimiters to separate individual histogram buckets and stores
 * a version number at the beginning of the encoded data.
 */
public class HistogramCodec {
    private static final byte[] CRLF = new byte[]{13, 10};

    /**
     * Encodes a histogram with a version number.
     * Format: [version (8 bytes)][bucket1][CRLF][bucket2][CRLF]...[bucketN][CRLF]
     *
     * @param histogram the histogram to encode
     * @param version   the version number to store with the histogram
     * @return the encoded byte array
     */
    public static byte[] encode(Histogram histogram, long version) {
        int total = 0;
        byte[][] items = new byte[histogram.size()][];
        for (int i = 0; i < histogram.size(); i++) {
            HistogramBucket bucket = histogram.get(i);
            byte[] data = HistogramBucketCodec.encode(bucket);
            total += data.length;
            total += CRLF.length;
            items[i] = data;
        }

        total += 8; // version
        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.putLong(version);
        for (byte[] item : items) {
            buffer.put(item);
            buffer.put(CRLF);
        }
        return buffer.array();
    }

    private static long readVersion(ByteBuffer buffer) {
        return buffer.getLong();
    }

    /**
     * Reads the version number from encoded histogram data without decoding the buckets.
     * Extracts the first 8 bytes as a long value.
     *
     * @param data the encoded histogram data
     * @return the version number
     */
    public static long readVersion(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return readVersion(buffer);
    }

    /**
     * Decodes histogram data into histogram buckets.
     * Skips the version number and parses CRLF-delimited bucket data.
     *
     * @param data the encoded histogram data
     * @return the decoded histogram buckets
     */
    public static Histogram decode(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        long version = readVersion(buffer);

        Histogram histogram = Histogram.create(version);
        while (buffer.hasRemaining()) {
            int start = buffer.position();
            int crlfPosition = findCRLF(buffer, start);

            if (crlfPosition == -1) {
                break;
            }

            int length = crlfPosition - start;
            byte[] bucketData = new byte[length];
            buffer.get(bucketData);

            HistogramBucket bucket = HistogramBucketCodec.decode(bucketData);
            histogram.add(bucket);

            buffer.get(); // Skip CR
            buffer.get(); // Skip LF
        }

        return histogram;
    }

    /**
     * Finds the position of the next CRLF delimiter in the buffer.
     *
     * @param buffer the byte buffer to search
     * @param start  the starting position for the search
     * @return the position of CR byte if found, -1 otherwise
     */
    private static int findCRLF(ByteBuffer buffer, int start) {
        for (int i = start; i < buffer.limit() - 1; i++) {
            if (buffer.get(i) == CRLF[0] && buffer.get(i + 1) == CRLF[1]) {
                return i;
            }
        }
        return -1;
    }
}
