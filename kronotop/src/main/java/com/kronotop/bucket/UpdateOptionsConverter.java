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

import com.kronotop.bucket.pipeline.ArrayFilter;
import com.kronotop.bucket.pipeline.UpdateOptions;
import com.kronotop.bucket.planner.Operator;
import org.bson.*;

import java.util.List;

/**
 * Converts BSON update documents into {@link UpdateOptions} instances.
 * <p>
 * This converter parses update operation documents that contain:
 * <ul>
 *   <li>{@code $set} - field updates as a document of field-value pairs</li>
 *   <li>{@code $unset} - field removals as an array of field names</li>
 *   <li>{@code array_filters} - conditional array element filters</li>
 *   <li>{@code upsert} - boolean flag to insert if no match found</li>
 * </ul>
 *
 * <p>Example input document:
 * <pre>{@code
 * {
 *   "$set": { "name": "Alice", "age": 30 },
 *   "$unset": ["oldField"],
 *   "array_filters": [{ "elem.score": { "$gte": 80 } }],
 *   "upsert": true
 * }
 * }</pre>
 */
public class UpdateOptionsConverter {

    /**
     * Converts a BSON update document into an {@link UpdateOptions} instance.
     *
     * @param updateDoc the BSON document containing update operations ({@code $set}, {@code $unset},
     *                  {@code array_filters}, {@code upsert})
     * @return a configured {@link UpdateOptions} instance
     * @throws IllegalArgumentException if the document contains invalid values (e.g., non-string unset keys,
     *                                  non-boolean upsert, malformed array filters)
     */
    public static UpdateOptions fromDocument(BsonDocument updateDoc) {
        UpdateOptions.Builder builder = UpdateOptions.builder();

        for (String key : updateDoc.keySet()) {
            Object value = updateDoc.get(key);

            switch (key.toLowerCase()) {
                case UpdateOptions.SET -> {
                    if (value instanceof BsonDocument setDoc) {
                        // Handle Document format: Document{{likes=2}}
                        for (String field : setDoc.keySet()) {
                            if (field.equals(ReservedFieldName.ID.getValue())) {
                                throw new IllegalArgumentException("_id field is immutable and cannot be modified with $set");
                            }
                            Object fieldValue = setDoc.get(field);
                            if (fieldValue instanceof BsonValue bsonValue) {
                                builder.set(field, bsonValue);
                            } else {
                                builder.set(field, BSONUtil.toBsonValue(fieldValue));
                            }
                        }
                    }
                }
                case UpdateOptions.UNSET -> {
                    if (value instanceof BsonDocument unsetDoc) {
                        // Handle Document format: {"field1": 1, "field2": 1} (values are ignored)
                        for (String field : unsetDoc.keySet()) {
                            builder.unset(field);
                        }
                    } else if (value instanceof BsonArray bsonArray) {
                        // Handle BsonArray format: BsonArray{values=[BsonString{value='field1'}]}
                        // Check BsonArray first since it also implements List
                        for (BsonValue bsonValue : bsonArray) {
                            if (bsonValue instanceof BsonString bsonString) {
                                builder.unset(bsonString.getValue());
                            } else {
                                throw new IllegalArgumentException("unset key must be a string, got: " + bsonValue.getClass().getSimpleName());
                            }
                        }
                    } else if (value instanceof List<?> unsetKeys) {
                        // Handle List format: [field1, field2] (includes ArrayList, List.of(), etc.)
                        for (Object unsetKey : unsetKeys) {
                            if (!(unsetKey instanceof String)) {
                                throw new IllegalArgumentException("unset key must be a string");
                            }
                            builder.unset((String) unsetKey);
                        }
                    }
                }
                case UpdateOptions.ARRAY_FILTERS -> {
                    if (value instanceof BsonArray bsonArray) {
                        for (BsonValue filterValue : bsonArray) {
                            if (!(filterValue instanceof BsonDocument filterDoc)) {
                                throw new IllegalArgumentException("array_filters element must be a document");
                            }
                            ArrayFilter filter = parseArrayFilter(filterDoc);
                            builder.arrayFilter(filter);
                        }
                    }
                }
                case UpdateOptions.UPSERT -> {
                    if (value instanceof BsonBoolean bsonBool) {
                        builder.upsert(bsonBool.getValue());
                    } else {
                        throw new IllegalArgumentException("upsert must be a boolean");
                    }
                }
            }
        }

        return builder.build();
    }

    /**
     * Parses a single array filter document.
     * Supports formats:
     * - { "identifier": { "$gte": 8 } } → operator with operand
     * - { "identifier": 5 } → implicit $eq
     */
    private static ArrayFilter parseArrayFilter(BsonDocument filterDoc) {
        if (filterDoc.size() != 1) {
            throw new IllegalArgumentException("Array filter document must have exactly one key (the identifier)");
        }

        String identifier = filterDoc.keySet().iterator().next();
        BsonValue filterValue = filterDoc.get(identifier);

        if (filterValue instanceof BsonDocument operatorDoc) {
            // Operator format: { "$gte": 8 }
            if (operatorDoc.size() != 1) {
                throw new IllegalArgumentException("Array filter operator document must have exactly one operator");
            }
            String opKey = operatorDoc.keySet().iterator().next();
            BsonValue operand = operatorDoc.get(opKey);

            Operator op = parseOperator(opKey);
            Object operandValue = convertBsonValueToObject(operand);
            return new ArrayFilter(identifier, op, operandValue);
        } else {
            // Implicit $eq: { "identifier": 5 }
            Object operandValue = convertBsonValueToObject(filterValue);
            return new ArrayFilter(identifier, Operator.EQ, operandValue);
        }
    }

    /**
     * Parses an operator string into an {@link Operator} enum value.
     *
     * @param opKey the operator string (e.g., "$eq", "$gte", "$in")
     * @return the corresponding {@link Operator} enum value
     * @throws IllegalArgumentException if the operator is not supported
     */
    private static Operator parseOperator(String opKey) {
        return switch (opKey.toLowerCase()) {
            case "$eq" -> Operator.EQ;
            case "$ne" -> Operator.NE;
            case "$gt" -> Operator.GT;
            case "$gte" -> Operator.GTE;
            case "$lt" -> Operator.LT;
            case "$lte" -> Operator.LTE;
            case "$in" -> Operator.IN;
            case "$nin" -> Operator.NIN;
            case "$all" -> Operator.ALL;
            case "$size" -> Operator.SIZE;
            case "$exists" -> Operator.EXISTS;
            default -> throw new IllegalArgumentException("Unsupported array filter operator: " + opKey);
        };
    }

    /**
     * Converts a {@link BsonValue} to its corresponding Java object representation.
     * Primitive types (INT32, INT64, DOUBLE, STRING, BOOLEAN) are unwrapped to Java primitives.
     * Complex types (DATE_TIME, TIMESTAMP, BINARY, ARRAY, DOCUMENT) are returned as their BSON wrappers.
     *
     * @param bsonValue the BSON value to convert
     * @return the Java object representation
     * @throws IllegalArgumentException if the BSON type is not supported
     */
    private static Object convertBsonValueToObject(BsonValue bsonValue) {
        return switch (bsonValue.getBsonType()) {
            case INT32 -> bsonValue.asInt32().getValue();
            case INT64 -> bsonValue.asInt64().getValue();
            case DOUBLE -> bsonValue.asDouble().getValue();
            case STRING -> bsonValue.asString().getValue();
            case BOOLEAN -> bsonValue.asBoolean().getValue();
            case NULL -> null;
            case DECIMAL128 -> bsonValue.asDecimal128().decimal128Value().bigDecimalValue();
            case OBJECT_ID -> bsonValue.asObjectId().getValue();
            // Return BsonValue directly for types that share Java primitives
            case DATE_TIME -> bsonValue.asDateTime();
            case TIMESTAMP -> bsonValue.asTimestamp();
            case BINARY -> bsonValue.asBinary();
            case ARRAY -> bsonValue.asArray();
            case DOCUMENT -> bsonValue.asDocument();
            default ->
                    throw new IllegalArgumentException("Unsupported array filter operand type: " + bsonValue.getBsonType());
        };
    }
}