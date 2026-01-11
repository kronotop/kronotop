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

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.BSONUtil;
import com.kronotop.internal.StringUtil;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class BSONUpdateUtil {

    private BSONUpdateUtil() {
        // Utility class
    }

    /**
     * Updates a BSON document by applying the set operations from UpdateOptions.
     * This method traverses the setOps map and updates or adds the corresponding fields in the document.
     *
     * @param document The original BSON document as ByteBuffer
     * @param setOps   Map of field names to new values
     * @return DocumentUpdateResult containing the updated document and modified fields
     */
    public static DocumentUpdateResult applyUpdateOperations(ByteBuffer document, Map<String, BsonValue> setOps, Set<String> unsetOps) {
        if (setOps.isEmpty() && unsetOps.isEmpty()) {
            return new DocumentUpdateResult(document, Map.of(), Set.of());
        }

        document.rewind();
        BsonDocument bsonDoc = BSONUtil.fromBson(document);

        Set<String> droppedSelectors = new HashSet<>();
        for (String selector : unsetOps) {
            if (unsetField(bsonDoc, selector)) {
                droppedSelectors.add(selector);
            }
        }

        Map<String, BsonValue> newValues = new HashMap<>();

        // Apply set operations and track the new values
        for (Map.Entry<String, BsonValue> setOp : setOps.entrySet()) {
            String fieldName = setOp.getKey();
            BsonValue bsonValue = setOp.getValue();

            // Track the new value being set (both new and updated fields)
            newValues.put(fieldName, bsonValue);

            bsonDoc.put(fieldName, bsonValue);
        }

        ByteBuffer updatedDocument = BSONUtil.toByteBuffer(bsonDoc);
        return new DocumentUpdateResult(updatedDocument, newValues, droppedSelectors);
    }

    /**
     * Unsets a field from a document, supporting dot-path notation for nested fields.
     * When the path traverses an array, the field is removed from each document element in the array.
     *
     * @param doc      the document to modify
     * @param selector the dot-path selector (e.g., "tags.name" to remove "name" from each element in "tags" array)
     * @return true if any field was removed, false otherwise
     */
    private static boolean unsetField(BsonDocument doc, String selector) {
        String[] pathSegments = StringUtil.split(selector);

        if (pathSegments.length == 1) {
            // Root level field
            if (doc.containsKey(selector)) {
                doc.remove(selector);
                return true;
            }
            return false;
        }

        // Navigate to parent, handling arrays
        return unsetNestedField(doc, pathSegments, 0);
    }

    private static boolean unsetNestedField(Object current, String[] pathSegments, int index) {
        if (current == null) {
            return false;
        }

        String segment = pathSegments[index];
        boolean isLastSegment = index == pathSegments.length - 1;

        switch (current) {
            case BsonDocument bsonDoc -> {
                if (!bsonDoc.containsKey(segment)) {
                    return false;
                }

                if (isLastSegment) {
                    bsonDoc.remove(segment);
                    return true;
                }

                BsonValue next = bsonDoc.get(segment);
                return unsetNestedField(next, pathSegments, index + 1);
            }
            case BsonArray bsonArray -> {
                // For BsonArrays, apply the unset to each document element
                boolean anyRemoved = false;
                for (BsonValue element : bsonArray) {
                    if (unsetNestedField(element, pathSegments, index)) {
                        anyRemoved = true;
                    }
                }
                return anyRemoved;
            }
            default -> {
                return false;
            }
        }
    }

    /**
     * Record to hold the result of a BSON document update operation.
     *
     * @param document  The updated BSON document as ByteBuffer
     * @param newValues Map of field names to their new BSON values that were set (both new and updated fields)
     */
    public record DocumentUpdateResult(ByteBuffer document, Map<String, BsonValue> newValues,
                                       Set<String> droppedSelectors) {
    }
}