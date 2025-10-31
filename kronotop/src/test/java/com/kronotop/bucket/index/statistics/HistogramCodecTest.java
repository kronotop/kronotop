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

import org.bson.BsonInt64;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HistogramCodecTest {
    @Test
    void shouldEncodeDecode_INT64() {
        List<HistogramBucket> histogram = List.of(
                new HistogramBucket(new BsonInt64(10), new BsonInt64(34), 10),
                new HistogramBucket(new BsonInt64(35), new BsonInt64(45), 10)
        );
        byte[] encoded = HistogramCodec.encode(histogram, 12345L);
        List<HistogramBucket> decoded = HistogramCodec.decode(encoded);
        assertEquals(histogram, decoded);
    }

    @Test
    void shouldEncodeDecode_STRING() {
        List<HistogramBucket> histogram = List.of(
                new HistogramBucket(new BsonString("apple"), new BsonString("banana"), 5),
                new HistogramBucket(new BsonString("cherry"), new BsonString("date"), 8),
                new HistogramBucket(new BsonString("elderberry"), new BsonString("fig"), 12)
        );
        byte[] encoded = HistogramCodec.encode(histogram, 67890L);
        List<HistogramBucket> decoded = HistogramCodec.decode(encoded);
        assertEquals(histogram, decoded);
    }

    @Test
    void shouldEncodeDecodeEmptyHistogram() {
        List<HistogramBucket> histogram = List.of();
        byte[] encoded = HistogramCodec.encode(histogram, 0L);
        List<HistogramBucket> decoded = HistogramCodec.decode(encoded);
        assertEquals(histogram, decoded);
    }

    @Test
    void shouldReadVersion() {
        List<HistogramBucket> histogram = List.of(
                new HistogramBucket(new BsonInt64(10), new BsonInt64(34), 10)
        );
        long expectedVersion = 98765L;
        byte[] encoded = HistogramCodec.encode(histogram, expectedVersion);
        long actualVersion = HistogramCodec.readVersion(encoded);
        assertEquals(expectedVersion, actualVersion);
    }
}