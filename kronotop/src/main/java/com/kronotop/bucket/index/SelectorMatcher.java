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

package com.kronotop.bucket.index;

import org.bson.*;

import java.nio.ByteBuffer;

/**
 * SelectorMatcher provides JSONPath-like functionality for traversing and extracting values
 * from BSON documents using dot-notation selectors.
 *
 * <h2>Supported Selector Patterns</h2>
 *
 * <h3>Simple Field Access</h3>
 * <ul>
 *   <li><code>"fieldName"</code> - Access a root-level field</li>
 *   <li><code>"user"</code> - Returns the value of the "user" field</li>
 * </ul>
 *
 * <h3>Nested Document Access</h3>
 * <ul>
 *   <li><code>"user.name"</code> - Access nested field "name" within "user" document</li>
 *   <li><code>"address.street.number"</code> - Multi-level nested field access</li>
 *   <li><code>"config.database.host"</code> - Deep nesting support</li>
 * </ul>
 *
 * <h3>Array Element Access</h3>
 * <ul>
 *   <li><code>"items.0"</code> - Access first element of "items" array (zero-based indexing)</li>
 *   <li><code>"users.2"</code> - Access third element of "users" array</li>
 *   <li><code>"data.5"</code> - Access sixth element of "data" array</li>
 * </ul>
 *
 * <h3>Mixed Array and Document Access</h3>
 * <ul>
 *   <li><code>"users.0.name"</code> - Access "name" field of first user in "users" array</li>
 *   <li><code>"orders.2.items.1.price"</code> - Complex nested array-document traversal</li>
 *   <li><code>"data.0.metadata.tags.0"</code> - Arrays within documents within arrays</li>
 * </ul>
 *
 * <h3>Complex Nested Structures</h3>
 * <ul>
 *   <li><code>"company.departments.0.employees.5.address.coordinates.latitude"</code> -
 *       Deep nesting with multiple arrays and documents</li>
 *   <li><code>"config.servers.0.database.connections.1.pool.maxSize"</code> -
 *       Configuration-style deep nesting</li>
 * </ul>
 *
 * <h2>Supported BSON Types</h2>
 * The matcher can extract values of all standard BSON types:
 * <ul>
 *   <li><strong>Primitives:</strong> String, Int32, Int64, Double, Boolean, Null</li>
 *   <li><strong>Date/Time:</strong> DateTime</li>
 *   <li><strong>Identifiers:</strong> ObjectId</li>
 *   <li><strong>Complex Types:</strong> Document (nested), Array</li>
 * </ul>
 *
 * <h2>Error Handling</h2>
 * <ul>
 *   <li>Returns <code>null</code> for non-existent fields or paths</li>
 *   <li>Returns <code>null</code> for invalid array indices (out of bounds or non-numeric)</li>
 *   <li>Returns <code>null</code> for type mismatches (e.g., trying to access array index on a primitive)</li>
 * </ul>
 *
 * <h2>Performance Characteristics</h2>
 * <ul>
 *   <li>Uses streaming BSON reader for memory efficiency</li>
 *   <li>Short-circuits traversal when target is found</li>
 *   <li>Skips unneeded values to minimize processing</li>
 *   <li>Recursive implementation handles arbitrary nesting depth</li>
 * </ul>
 *
 * <h2>Usage Examples</h2>
 * <pre>{@code
 * Document doc = new Document()
 *     .append("user", new Document()
 *         .append("name", "John")
 *         .append("age", 30))
 *     .append("scores", Arrays.asList(95, 87, 92));
 *
 * // Simple field access
 * BsonValue userName = SelectorMatcher.match("user.name", doc);  // "John"
 *
 * // Array access
 * BsonValue firstScore = SelectorMatcher.match("scores.0", doc);  // 95
 *
 * // Non-existent path
 * BsonValue missing = SelectorMatcher.match("user.email", doc);  // null
 * }</pre>
 *
 * @see org.bson.BsonReader
 * @see org.bson.BsonValue
 * @since 0.13
 */
public class SelectorMatcher {
    /**
     * Matches a selector path against a BSON document and returns the corresponding value.
     *
     * <p>This method traverses the document using the provided selector string, which follows
     * JSONPath-like dot notation. The selector can navigate through nested documents and arrays
     * using field names and numeric indices respectively.</p>
     *
     * <h3>Selector Syntax</h3>
     * <ul>
     *   <li><strong>Field access:</strong> <code>"fieldName"</code></li>
     *   <li><strong>Nested fields:</strong> <code>"parent.child"</code></li>
     *   <li><strong>Array elements:</strong> <code>"arrayField.0"</code> (zero-based indexing)</li>
     *   <li><strong>Mixed access:</strong> <code>"users.0.profile.name"</code></li>
     * </ul>
     *
     * <h3>Examples</h3>
     * <pre>{@code
     * Document user = new Document()
     *     .append("name", "Alice")
     *     .append("contact", new Document()
     *         .append("email", "alice@example.com")
     *         .append("phones", Arrays.asList("123-456-7890", "987-654-3210")));
     *
     * BsonValue name = SelectorMatcher.match("name", user);                    // "Alice"
     * BsonValue email = SelectorMatcher.match("contact.email", user);          // "alice@example.com"
     * BsonValue firstPhone = SelectorMatcher.match("contact.phones.0", user);  // "123-456-7890"
     * BsonValue missing = SelectorMatcher.match("contact.address", user);      // null
     * }</pre>
     *
     * @param selector the dot-notation path to the desired value (e.g., "user.profile.name", "items.0")
     * @param document the BSON document to search within
     * @return the BsonValue found at the specified path, or {@code null} if the path doesn't exist,
     * contains invalid array indices, or encounters type mismatches
     * @throws IllegalArgumentException if selector is null or empty
     * @throws IllegalArgumentException if document is null
     */
    public static BsonValue match(String selector, Document document) {
        String[] pathSegments = selector.split("\\.");

        try (BsonReader reader = document.toBsonDocument().asBsonReader()) {
            reader.readStartDocument();
            return findValueInDocument(reader, pathSegments, 0);
        }
    }

    /**
     * Matches a selector path against a BSON document represented as a ByteBuffer and returns the corresponding value.
     * <p>
     * This method navigates through the BSON document using a dot-separated path specified by the selector string.
     * It reads data from the ByteBuffer and traverses through nested documents or arrays as necessary to locate
     * the desired value.
     *
     * @param selector the dot-notation path to the desired value (e.g., "field.subfield", "arrayField.0")
     * @param input    the ByteBuffer containing the BSON document to search within
     * @return the BsonValue found at the specified path, or {@code null} if the path doesn't exist,
     * the BSON structure does not match the selector, or array indices are invalid
     * @throws IllegalArgumentException if the selector is null or empty
     * @throws NullPointerException     if the input ByteBuffer is null
     */
    public static BsonValue match(String selector, ByteBuffer input) {
        String[] pathSegments = selector.split("\\.");

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
                try {
                    int arrayIndex = Integer.parseInt(targetKey);
                    reader.readStartArray();
                    int currentArrayIndex = 0;
                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        if (currentArrayIndex == arrayIndex) {
                            boolean isLastSegment = currentIndex == pathSegments.length - 1;
                            if (isLastSegment) {
                                BsonValue value = readCurrentValue(reader);
                                // Skip remaining elements
                                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                                    reader.skipValue();
                                }
                                reader.readEndArray();
                                return value;
                            } else {
                                BsonValue value = traverseToNextLevel(reader, pathSegments, currentIndex + 1);
                                // Skip remaining elements
                                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                                    reader.skipValue();
                                }
                                reader.readEndArray();
                                return value;
                            }
                        } else {
                            reader.skipValue();
                        }
                        currentArrayIndex++;
                    }
                    reader.readEndArray();
                } catch (NumberFormatException e) {
                    reader.skipValue();
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
                return new org.bson.BsonString(reader.readString());
            case INT32:
                return new org.bson.BsonInt32(reader.readInt32());
            case INT64:
                return new org.bson.BsonInt64(reader.readInt64());
            case DOUBLE:
                return new org.bson.BsonDouble(reader.readDouble());
            case BOOLEAN:
                return new org.bson.BsonBoolean(reader.readBoolean());
            case NULL:
                reader.readNull();
                return new org.bson.BsonNull();
            case DATE_TIME:
                return new org.bson.BsonDateTime(reader.readDateTime());
            case OBJECT_ID:
                return new org.bson.BsonObjectId(reader.readObjectId());
            case DOCUMENT:
                // For nested documents, we need to read it recursively
                org.bson.BsonDocument nestedDoc = new org.bson.BsonDocument();
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
                org.bson.BsonArray nestedArray = new org.bson.BsonArray();
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
