/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.bucket.index;

import com.kronotop.internal.StringUtil;
import org.bson.*;

import java.nio.ByteBuffer;

/**
 * Extracts a value from a BSON document using a dot-notation selector.
 *
 * <p>A selector names a path into the document. Field names step into nested
 * documents, numeric segments index into arrays with zero-based indexing. So
 * {@code "user.name"} reads the "name" field of the "user" document, and
 * {@code "orders.0.total"} reads the "total" field of the first order.
 *
 * <p>When a non-numeric segment is applied to an array, the matcher walks every
 * element and collects the matching values into an array. This multikey traversal
 * is what lets an array-valued field feed a multikey index.
 *
 * <p>The document is read with a streaming BSON reader, so traversal stops once
 * the target is reached. A missing field, an out-of-range array index, or a type
 * mismatch returns {@code null}.
 *
 * @see org.bson.BsonReader
 * @see org.bson.BsonValue
 * @since 0.13
 */
public class SelectorMatcher {
    /**
     * Extracts the value at the given dot-notation selector from a document.
     *
     * @param selector the dot-notation path, for example "user.profile.name" or "items.0"
     * @param document the document to read
     * @return the value at the path, or {@code null} if the path does not exist, an array index
     * is out of range, or a segment hits a type mismatch
     * @throws IllegalArgumentException if the selector is null or empty
     * @throws NullPointerException     if the document is null
     */
    public static BsonValue match(String selector, BsonDocument document) {
        return match(StringUtil.split(selector), document);
    }

    /**
     * Matches pre-split path segments against a BSON document and returns the corresponding value.
     *
     * @param pathSegments the pre-split selector path segments
     * @param document     the BSON document to search within
     * @return the BsonValue found at the specified path, or {@code null} if the path doesn't exist
     */
    public static BsonValue match(String[] pathSegments, BsonDocument document) {
        try (BsonReader reader = document.asBsonReader()) {
            reader.readStartDocument();
            return findValueInDocument(reader, pathSegments, 0);
        }
    }

    /**
     * Matches a selector path against a BSON document held in a ByteBuffer and returns the corresponding value.
     *
     * @param selector the dot-notation path to the desired value (e.g., "field.subfield", "arrayField.0")
     * @param input    the ByteBuffer containing the BSON document to search within
     * @return the BsonValue found at the specified path, or {@code null} if the path doesn't exist,
     * the BSON structure does not match the selector, or array indices are invalid
     * @throws IllegalArgumentException if the selector is null or empty
     * @throws NullPointerException     if the input ByteBuffer is null
     */
    public static BsonValue match(String selector, ByteBuffer input) {
        return match(StringUtil.split(selector), input);
    }

    /**
     * Matches pre-split path segments against a BSON document represented as a ByteBuffer.
     *
     * @param pathSegments the pre-split selector path segments
     * @param input        the ByteBuffer containing the BSON document to search within
     * @return the BsonValue found at the specified path, or {@code null} if the path doesn't exist
     */
    public static BsonValue match(String[] pathSegments, ByteBuffer input) {
        try (BsonReader reader = new BsonBinaryReader(input)) {
            reader.readStartDocument();
            return findValueInDocument(reader, pathSegments, 0);
        } finally {
            input.rewind();
        }
    }

    private static BsonValue findValueInDocument(BsonReader reader, String[] pathSegments, int currentIndex) {
        if (currentIndex >= pathSegments.length) {
            return null;
        }

        String targetKey = pathSegments[currentIndex];
        boolean isLastSegment = currentIndex == pathSegments.length - 1;
        BsonValue result = null;

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String key = reader.readName();
            if (key.equals(targetKey)) {
                if (isLastSegment) {
                    result = readCurrentValue(reader);
                } else {
                    result = traverseToNextLevel(reader, pathSegments, currentIndex + 1);
                }
                // Found the target, but we still need to consume remaining fields
                break;
            } else {
                reader.skipValue();
            }
        }

        // Consume any remaining fields if we found a result early
        if (result != null) {
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                reader.readName();
                reader.skipValue();
            }
        }

        return result;
    }

    private static BsonValue traverseToNextLevel(BsonReader reader, String[] pathSegments, int currentIndex) {
        BsonType currentType = reader.getCurrentBsonType();

        switch (currentType) {
            case DOCUMENT:
                reader.readStartDocument();
                BsonValue result = findValueInDocument(reader, pathSegments, currentIndex);
                reader.readEndDocument();
                return result;

            case ARRAY:
                String targetKey = pathSegments[currentIndex];
                reader.readStartArray();

                // Check if targeting a specific array index
                Integer arrayIndex = null;
                try {
                    arrayIndex = Integer.parseInt(targetKey);
                } catch (NumberFormatException ignored) {
                }

                if (arrayIndex != null) {
                    // Numeric index: access specific array element
                    int currentArrayIndex = 0;
                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        if (currentArrayIndex == arrayIndex) {
                            boolean isLastSegment = currentIndex == pathSegments.length - 1;
                            BsonValue value;
                            if (isLastSegment) {
                                value = readCurrentValue(reader);
                            } else {
                                value = traverseToNextLevel(reader, pathSegments, currentIndex + 1);
                            }
                            // Skip remaining elements
                            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                                reader.skipValue();
                            }
                            reader.readEndArray();
                            return value;
                        } else {
                            reader.skipValue();
                        }
                        currentArrayIndex++;
                    }
                    reader.readEndArray();
                } else {
                    // Non-numeric key: iterate through array elements and collect matching values
                    BsonArray collectedValues = new BsonArray();
                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        if (reader.getCurrentBsonType() == BsonType.DOCUMENT) {
                            reader.readStartDocument();
                            BsonValue foundValue = findValueInDocument(reader, pathSegments, currentIndex);
                            reader.readEndDocument();
                            if (foundValue != null) {
                                // Flatten nested arrays to support multi-level array traversal
                                if (foundValue instanceof BsonArray nestedArray) {
                                    collectedValues.addAll(nestedArray);
                                } else {
                                    collectedValues.add(foundValue);
                                }
                            }
                        } else {
                            reader.skipValue();
                        }
                    }
                    reader.readEndArray();
                    return collectedValues.isEmpty() ? null : collectedValues;
                }
                break;

            default:
                reader.skipValue();
                break;
        }

        return null;
    }

    private static BsonValue readCurrentValue(BsonReader reader) {
        BsonType type = reader.getCurrentBsonType();

        switch (type) {
            case STRING:
                return new BsonString(reader.readString());
            case INT32:
                return new BsonInt32(reader.readInt32());
            case INT64:
                return new BsonInt64(reader.readInt64());
            case DOUBLE:
                return new BsonDouble(reader.readDouble());
            case BOOLEAN:
                return new BsonBoolean(reader.readBoolean());
            case NULL:
                reader.readNull();
                return new BsonNull();
            case DATE_TIME:
                return new BsonDateTime(reader.readDateTime());
            case TIMESTAMP:
                return new BsonTimestamp(reader.readTimestamp().getValue());
            case BINARY:
                BsonBinary binaryData = reader.readBinaryData();
                return new BsonBinary(binaryData.getType(), binaryData.getData());
            case DECIMAL128:
                return new BsonDecimal128(reader.readDecimal128());
            case OBJECT_ID:
                return new BsonObjectId(reader.readObjectId());
            case DOCUMENT:
                // For nested documents, we need to read them recursively
                BsonDocument nestedDoc = new BsonDocument();
                reader.readStartDocument();
                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    String fieldName = reader.readName();
                    BsonValue fieldValue = readCurrentValue(reader);
                    nestedDoc.put(fieldName, fieldValue);
                }
                reader.readEndDocument();
                return nestedDoc;
            case ARRAY:
                // For nested arrays, we need to read it recursively
                BsonArray nestedArray = new BsonArray();
                reader.readStartArray();
                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    BsonValue elementValue = readCurrentValue(reader);
                    nestedArray.add(elementValue);
                }
                reader.readEndArray();
                return nestedArray;
            default:
                reader.skipValue();
                return null;
        }
    }
}
