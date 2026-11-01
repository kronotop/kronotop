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

package com.kronotop.bucket;

import org.bson.*;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

/**
 * Orders {@link BsonValue} instances by type bracket first and by value inside the same bracket.
 * The bracket order is null, numeric, string, document, array, binary, ObjectId, boolean, date-time, timestamp,
 * and all numeric types share one bracket.
 */
public final class TypeBracketComparator implements Comparator<BsonValue> {

    public static final TypeBracketComparator INSTANCE = new TypeBracketComparator();

    private TypeBracketComparator() {
    }

    private int getTypeOrder(BsonType type) {
        return switch (type) {
            case NULL -> 0;
            case INT32, INT64, DOUBLE, DECIMAL128 -> 1;
            case STRING -> 2;
            case DOCUMENT -> 3;
            case ARRAY -> 4;
            case BINARY -> 5;
            case OBJECT_ID -> 6;
            case BOOLEAN -> 7;
            case DATE_TIME -> 8;
            case TIMESTAMP -> 9;
            default -> throw new IllegalArgumentException("Unsupported BSON type: " + type);
        };
    }

    /**
     * Compares two values by type bracket first and by value inside the same bracket, and treats Java null as BSON
     * null.
     *
     * @param a the first value
     * @param b the second value
     * @return negative if {@code a} is smaller, zero if they are equal, positive if {@code a} is larger
     * @throws IllegalArgumentException if either value has an unsupported BSON type
     */
    @Override
    public int compare(BsonValue a, BsonValue b) {
        if (a == null) {
            a = BsonNull.VALUE;
        }
        if (b == null) {
            b = BsonNull.VALUE;
        }

        int typeCompare = Integer.compare(getTypeOrder(a.getBsonType()), getTypeOrder(b.getBsonType()));
        if (typeCompare != 0) {
            return typeCompare;
        }

        if (a.getBsonType() != b.getBsonType() && NumericWidening.isNumericBsonType(a.getBsonType())) {
            return compareNumericValues(a, b);
        }

        return compareValues(a, b);
    }

    @SuppressWarnings("unchecked")
    private int compareNumericValues(BsonValue a, BsonValue b) {
        BsonType commonType = NumericWidening.commonType(a.getBsonType(), b.getBsonType());
        Object valA = NumericWidening.extractNumericValue(a);
        Object valB = NumericWidening.extractNumericValue(b);
        Comparable<Object> promotedA = (Comparable<Object>) NumericWidening.promoteToComparable(valA, a.getBsonType(), commonType);
        Comparable<Object> promotedB = (Comparable<Object>) NumericWidening.promoteToComparable(valB, b.getBsonType(), commonType);
        assert promotedA != null;
        assert promotedB != null;
        return promotedA.compareTo(promotedB);
    }

    private int compareValues(BsonValue a, BsonValue b) {
        return switch (a.getBsonType()) {
            case NULL -> 0;
            case INT32 -> Integer.compare(a.asInt32().getValue(), b.asInt32().getValue());
            case INT64 -> Long.compare(a.asInt64().getValue(), b.asInt64().getValue());
            case DOUBLE -> Double.compare(a.asDouble().getValue(), b.asDouble().getValue());
            case DECIMAL128 -> a.asDecimal128().decimal128Value().bigDecimalValue()
                    .compareTo(b.asDecimal128().decimal128Value().bigDecimalValue());
            case STRING -> a.asString().getValue().compareTo(b.asString().getValue());
            case DOCUMENT -> compareDocuments(a.asDocument(), b.asDocument());
            case ARRAY -> compareArrays(a.asArray(), b.asArray());
            case BINARY -> Arrays.compare(a.asBinary().getData(), b.asBinary().getData());
            case OBJECT_ID -> a.asObjectId().getValue().compareTo(b.asObjectId().getValue());
            case BOOLEAN -> Boolean.compare(a.asBoolean().getValue(), b.asBoolean().getValue());
            case DATE_TIME -> Long.compare(a.asDateTime().getValue(), b.asDateTime().getValue());
            case TIMESTAMP -> a.asTimestamp().compareTo(b.asTimestamp());
            default -> throw new IllegalArgumentException("Unsupported BSON type: " + a.getBsonType());
        };
    }

    private int compareDocuments(BsonDocument a, BsonDocument b) {
        Iterator<Map.Entry<String, BsonValue>> iterA = a.entrySet().iterator();
        Iterator<Map.Entry<String, BsonValue>> iterB = b.entrySet().iterator();

        while (iterA.hasNext() && iterB.hasNext()) {
            Map.Entry<String, BsonValue> entryA = iterA.next();
            Map.Entry<String, BsonValue> entryB = iterB.next();

            int keyCompare = entryA.getKey().compareTo(entryB.getKey());
            if (keyCompare != 0) {
                return keyCompare;
            }

            int valueCompare = compare(entryA.getValue(), entryB.getValue());
            if (valueCompare != 0) {
                return valueCompare;
            }
        }

        return Integer.compare(a.size(), b.size());
    }

    private int compareArrays(BsonArray a, BsonArray b) {
        int minSize = Math.min(a.size(), b.size());
        for (int i = 0; i < minSize; i++) {
            int cmp = compare(a.get(i), b.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(a.size(), b.size());
    }
}
