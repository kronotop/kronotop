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

import org.bson.BsonType;
import org.bson.BsonValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.EnumMap;

public class HistogramBucketCodec {
    private static final EnumMap<BsonType, Integer> FIXED_CAPACITY = new EnumMap<>(BsonType.class);

    static {
        FIXED_CAPACITY.put(BsonType.INT64, 21);
    }

    private static void encodeBsonValue(ByteBuffer buffer, BsonValue value) {
        switch (value.getBsonType()) {
            case INT64 -> {
                buffer.putLong(value.asInt64().getValue());
            }
            default -> throw new IllegalArgumentException("Unknown BsonValue type: " + value.getBsonType());
        }
    }

    public static byte[] encode(HistogramBucket histogramBucket) {
        if (histogramBucket.min().getBsonType() != histogramBucket.max().getBsonType()) {
            throw new IllegalStateException("min and max must have the same BsonType");
        }

        BsonType bsonType = histogramBucket.min().getBsonType();
        int capacity = FIXED_CAPACITY.get(bsonType);
        ByteBuffer buffer = ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);

        buffer.put((byte) bsonType.getValue());
        encodeBsonValue(buffer, histogramBucket.min());
        encodeBsonValue(buffer, histogramBucket.max());
        buffer.putInt(histogramBucket.count());

        return buffer.array();
    }

    public static HistogramBucket decode(byte[] input) {
        ByteBuffer buffer = ByteBuffer.wrap(input);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        byte typeByte = buffer.get();
        BsonType bsonType = BsonType.findByValue(typeByte);
        if (bsonType == null) {
            throw new IllegalArgumentException("Unknown BsonType: " + typeByte);
        }

        switch (bsonType) {
            case INT64 -> {
                long min = buffer.getLong();
                long max = buffer.getLong();
                int count = buffer.getInt();

                return new HistogramBucket(
                        new org.bson.BsonInt64(min),
                        new org.bson.BsonInt64(max),
                        count
                );
            }
            default -> throw new IllegalArgumentException("Unsupported BsonType: " + bsonType);
        }
    }
}
