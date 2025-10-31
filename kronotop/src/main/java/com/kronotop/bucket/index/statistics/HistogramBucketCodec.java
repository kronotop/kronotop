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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.EnumMap;

public class HistogramBucketCodec {
    private static final EnumMap<BsonType, Integer> FIXED_CAPACITY = new EnumMap<>(BsonType.class);

    static {
        FIXED_CAPACITY.put(BsonType.INT64, 21);
        FIXED_CAPACITY.put(BsonType.INT32, 13);
        FIXED_CAPACITY.put(BsonType.DOUBLE, 21);
        FIXED_CAPACITY.put(BsonType.DATE_TIME, 21);
        FIXED_CAPACITY.put(BsonType.TIMESTAMP, 21);
    }

    private static void encodeBsonValue(ByteBuffer buffer, BsonValue value) {
        switch (value.getBsonType()) {
            case INT64 -> buffer.putLong(value.asInt64().getValue());
            case INT32 -> buffer.putInt(value.asInt32().getValue());
            case DOUBLE -> buffer.putDouble(value.asDouble().getValue());
            case DATE_TIME -> buffer.putLong(value.asDateTime().getValue());
            case TIMESTAMP -> {
                buffer.putInt(value.asTimestamp().getTime());
                buffer.putInt(value.asTimestamp().getInc());
            }
            case BINARY -> {
                byte[] data = value.asBinary().getData();
                buffer.putShort((short) data.length);
                buffer.put(data);
            }
            case STRING -> {
                byte[] data = value.asString().getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                buffer.putShort((short) data.length);
                buffer.put(data);
            }
            default -> throw new IllegalArgumentException("Unknown BsonValue type: " + value.getBsonType());
        }
    }

    private static int calculateCapacity(HistogramBucket histogramBucket) {
        BsonType bsonType = histogramBucket.min().getBsonType();

        if (bsonType == BsonType.BINARY) {
            int minLength = histogramBucket.min().asBinary().getData().length;
            int maxLength = histogramBucket.max().asBinary().getData().length;
            // BsonType(byte) + min-length(short) + min-length-itself + max-length(short) + min-length-itself + int
            return 1 + 2 + minLength + 2 + maxLength + 4;
        }

        if (bsonType == BsonType.STRING) {
            int minLength = histogramBucket.min().asString().getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            int maxLength = histogramBucket.max().asString().getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            // BsonType(byte) + min-length(short) + min-length-itself + max-length(short) + min-length-itself + int
            return 1 + 2 + minLength + 2 + maxLength + 4;
        }

        return FIXED_CAPACITY.get(bsonType);
    }

    public static byte[] encode(HistogramBucket histogramBucket) {
        if (histogramBucket.min().getBsonType() != histogramBucket.max().getBsonType()) {
            throw new IllegalStateException("min and max must have the same BsonType");
        }

        BsonType bsonType = histogramBucket.min().getBsonType();
        int capacity = calculateCapacity(histogramBucket);
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
                        new BsonInt64(min),
                        new BsonInt64(max),
                        count
                );
            }
            case INT32 -> {
                int min = buffer.getInt();
                int max = buffer.getInt();
                int count = buffer.getInt();

                return new HistogramBucket(
                        new BsonInt32(min),
                        new BsonInt32(max),
                        count
                );
            }
            case DOUBLE -> {
                double min = buffer.getDouble();
                double max = buffer.getDouble();
                int count = buffer.getInt();

                return new HistogramBucket(
                        new BsonDouble(min),
                        new BsonDouble(max),
                        count
                );
            }
            case DATE_TIME -> {
                long min = buffer.getLong();
                long max = buffer.getLong();
                int count = buffer.getInt();

                return new HistogramBucket(
                        new BsonDateTime(min),
                        new BsonDateTime(max),
                        count
                );
            }
            case TIMESTAMP -> {
                int minTime = buffer.getInt();
                int minInc = buffer.getInt();
                int maxTime = buffer.getInt();
                int maxInc = buffer.getInt();
                int count = buffer.getInt();

                return new HistogramBucket(
                        new BsonTimestamp(minTime, minInc),
                        new BsonTimestamp(maxTime, maxInc),
                        count
                );
            }
            case BINARY -> {
                short minLength = buffer.getShort();
                byte[] minData = new byte[minLength];
                buffer.get(minData);

                short maxLength = buffer.getShort();
                byte[] maxData = new byte[maxLength];
                buffer.get(maxData);

                int count = buffer.getInt();

                return new HistogramBucket(
                        new BsonBinary(minData),
                        new BsonBinary(maxData),
                        count
                );
            }
            case STRING -> {
                short minLength = buffer.getShort();
                byte[] minData = new byte[minLength];
                buffer.get(minData);
                String minValue = new String(minData, java.nio.charset.StandardCharsets.UTF_8);

                short maxLength = buffer.getShort();
                byte[] maxData = new byte[maxLength];
                buffer.get(maxData);
                String maxValue = new String(maxData, java.nio.charset.StandardCharsets.UTF_8);

                int count = buffer.getInt();

                return new HistogramBucket(
                        new BsonString(minValue),
                        new BsonString(maxValue),
                        count
                );
            }
            default -> throw new IllegalArgumentException("Unsupported BsonType: " + bsonType);
        }
    }
}
