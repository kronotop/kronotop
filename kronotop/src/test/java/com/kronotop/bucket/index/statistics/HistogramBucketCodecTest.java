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

import org.bson.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HistogramBucketCodecTest {
    @Test
    void shouldEncodeDecode_INT64() {
        HistogramBucket bucket = new HistogramBucket(new BsonInt64(10), new BsonInt64(34), 10);
        byte[] data = HistogramBucketCodec.encode(bucket);
        HistogramBucket decoded = HistogramBucketCodec.decode(data);
        assertEquals(bucket, decoded);
    }

    @Test
    void shouldEncodeDecode_INT32() {
        HistogramBucket bucket = new HistogramBucket(new BsonInt32(10), new BsonInt32(34), 10);
        byte[] data = HistogramBucketCodec.encode(bucket);
        HistogramBucket decoded = HistogramBucketCodec.decode(data);
        assertEquals(bucket, decoded);
    }

    @Test
    void shouldEncodeDecode_DOUBLE() {
        HistogramBucket bucket = new HistogramBucket(new BsonDouble(10.5), new BsonDouble(34.7), 10);
        byte[] data = HistogramBucketCodec.encode(bucket);
        HistogramBucket decoded = HistogramBucketCodec.decode(data);
        assertEquals(bucket, decoded);
    }

    @Test
    void shouldEncodeDecode_DATE_TIME() {
        HistogramBucket bucket = new HistogramBucket(new BsonDateTime(1609459200000L), new BsonDateTime(1640995200000L), 10);
        byte[] data = HistogramBucketCodec.encode(bucket);
        HistogramBucket decoded = HistogramBucketCodec.decode(data);
        assertEquals(bucket, decoded);
    }

    @Test
    void shouldEncodeDecode_TIMESTAMP() {
        HistogramBucket bucket = new HistogramBucket(new BsonTimestamp(1609459200, 1), new BsonTimestamp(1640995200, 5), 10);
        byte[] data = HistogramBucketCodec.encode(bucket);
        HistogramBucket decoded = HistogramBucketCodec.decode(data);
        assertEquals(bucket, decoded);
    }

    @Test
    void shouldEncodeDecode_BINARY() {
        byte[] minBytes = {0x01, 0x02, 0x03, 0x04, 0x05};
        byte[] maxBytes = {0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F};
        HistogramBucket bucket = new HistogramBucket(new BsonBinary(minBytes), new BsonBinary(maxBytes), 10);
        byte[] data = HistogramBucketCodec.encode(bucket);
        HistogramBucket decoded = HistogramBucketCodec.decode(data);
        assertEquals(bucket, decoded);
    }

    @Test
    void shouldEncodeDecode_STRING() {
        HistogramBucket bucket = new HistogramBucket(new BsonString("apple"), new BsonString("zebra"), 10);
        byte[] data = HistogramBucketCodec.encode(bucket);
        HistogramBucket decoded = HistogramBucketCodec.decode(data);
        assertEquals(bucket, decoded);
    }
}