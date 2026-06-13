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

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.bson.BasicOutputBuffer;
import com.kronotop.internal.StringUtil;
import org.bson.*;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonReader;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Utility class for BSON document serialization and deserialization.
 * Provides methods for converting {@code BsonDocument} objects to byte arrays and vice versa.
 * <p>
 * Uses {@code BsonDocumentCodec} for encoding and decoding operations.
 */
public class BSONUtil {

    private static final EncoderContext ENCODER_CONTEXT = EncoderContext.builder().isEncodingCollectibleDocument(true).build();
    private static final Codec<BsonDocument> BSON_DOCUMENT_CODEC = new BsonDocumentCodec();

    /**
     * Serializes a {@code BsonDocument} to a BSON-encoded byte array.
     *
     * @param document the document to serialize
     * @return the serialized BSON data as a byte array
     */
    public static byte[] toBytes(BsonDocument document) {
        try (BasicOutputBuffer buffer = new BasicOutputBuffer()) {
            BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
            BSON_DOCUMENT_CODEC.encode(writer, document, ENCODER_CONTEXT);
            return buffer.toByteArray();
        }
    }

    /**
     * Serializes a {@code BsonDocument} to a BSON-encoded {@code ByteBuffer}.
     *
     * @param document the document to serialize
     * @return the serialized BSON data as a {@code ByteBuffer} ready for reading
     */
    public static ByteBuffer toByteBuffer(BsonDocument document) {
        return toByteBuffer(document, 1024);
    }

    /**
     * Serializes a {@code BsonDocument} to a BSON-encoded {@code ByteBuffer} with a pre-sized buffer.
     *
     * @param document    the document to serialize
     * @param initialSize initial buffer capacity in bytes
     * @return the serialized BSON data as a {@code ByteBuffer} ready for reading
     */
    public static ByteBuffer toByteBuffer(BsonDocument document, int initialSize) {
        BasicOutputBuffer buffer = new BasicOutputBuffer(initialSize);
        BSON_DOCUMENT_CODEC.encode(
                new BsonBinaryWriter(buffer),
                document,
                ENCODER_CONTEXT
        );
        // The internal buffer may include trailing bytes, use it wisely.
        return buffer.getInternalBuffer();
    }

    /**
     * Deserializes a BSON-encoded byte array into a {@code BsonDocument}.
     *
     * @param bytes the BSON-encoded byte array
     * @return the deserialized {@code BsonDocument}
     */
    public static BsonDocument toBsonDocument(byte[] bytes) {
        try (BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes))) {
            return BSON_DOCUMENT_CODEC.decode(reader, DecoderContext.builder().build());
        }
    }

    /**
     * Converts a JSON-encoded byte array into a {@code BsonDocument}.
     * If the {@code _id} field is a valid 24-character hex string, it is converted to {@code BsonObjectId}.
     *
     * @param bytes the JSON-encoded byte array
     * @return the decoded {@code BsonDocument}
     */
    public static BsonDocument fromJson(byte[] bytes) {
        Reader targetReader = new InputStreamReader(new ByteArrayInputStream(bytes));
        try (JsonReader reader = new JsonReader(targetReader)) {
            BsonDocument document = BSON_DOCUMENT_CODEC.decode(reader, DecoderContext.builder().build());
            coerceIdToObjectId(document);
            return document;
        }
    }

    private static void coerceIdToObjectId(BsonDocument document) {
        BsonValue idValue = document.get(ReservedFieldName.ID.getValue());
        if (idValue != null && idValue.isString()) {
            String idString = idValue.asString().getValue();
            if (ObjectId.isValid(idString)) {
                document.put(ReservedFieldName.ID.getValue(), new BsonObjectId(new ObjectId(idString)));
            }
        }
    }

    /**
     * Deserializes a byte array containing BSON data into a {@code BsonDocument}.
     *
     * @param bytes the byte array containing serialized BSON data
     * @return the deserialized {@code BsonDocument}
     */
    public static BsonDocument fromBson(byte[] bytes) {
        return fromBson(ByteBuffer.wrap(bytes));
    }

    /**
     * Converts a {@code BsonDocument} to a JSON string, serializing {@code _id} as a plain hex string.
     *
     * @param doc the document to convert
     * @return the JSON string representation
     */
    public static String toJson(BsonDocument doc) {
        BsonValue idValue = doc.get(ReservedFieldName.ID.getValue());
        if (idValue instanceof BsonObjectId value) {
            doc.put(ReservedFieldName.ID.getValue(), new BsonString(value.getValue().toHexString()));
        }
        return doc.toJson();
    }

    /**
     * Deserializes a {@code ByteBuffer} containing BSON data into a {@code BsonDocument}.
     *
     * @param buffer the {@code ByteBuffer} containing serialized BSON data
     * @return the deserialized {@code BsonDocument}
     */
    public static BsonDocument fromBson(ByteBuffer buffer) {
        try (BsonBinaryReader reader = new BsonBinaryReader(buffer)) {
            return BSON_DOCUMENT_CODEC.decode(reader, DecoderContext.builder().build());
        }
    }

    /**
     * Parses a JSON string and serializes it to a BSON byte array.
     *
     * @param data the JSON string to parse
     * @return a byte array containing the serialized BSON document
     */
    public static byte[] jsonToDocumentThenBytes(String data) {
        RawBsonDocument raw = RawBsonDocument.parse(data);
        coerceIdToObjectId(raw);
        return BSONUtil.toBytes(raw);
    }

    /**
     * Converts a Java object to its equivalent BSON value representation.
     *
     * @param value the Java object to convert to BsonValue
     * @return the equivalent BsonValue representation
     * @throws IllegalArgumentException if the value type is not supported for BSON conversion
     */
    public static BsonValue toBsonValue(Object value) {
        if (value == null) {
            return BsonNull.VALUE;
        }

        return switch (value) {
            case String str -> new BsonString(str);
            case Integer intVal -> new BsonInt32(intVal);
            case Long longVal -> new BsonInt64(longVal);
            case Double doubleVal -> new BsonDouble(doubleVal);
            case Boolean boolVal -> new BsonBoolean(boolVal);
            case Date dateVal -> new BsonDateTime(dateVal.getTime());
            case BigDecimal decimalVal -> new BsonDecimal128(new Decimal128(decimalVal));
            case Decimal128 decimal128Val -> new BsonDecimal128(decimal128Val);
            case byte[] binaryVal -> new BsonBinary(binaryVal);
            case Binary binaryVal -> new BsonBinary(binaryVal.getData());
            case BsonValue bsonVal -> bsonVal; // Already a BsonValue
            case Document docVal -> docVal.toBsonDocument(); // Convert Document to BsonDocument
            case ObjectId objectIdVal -> new BsonObjectId(objectIdVal);
            case Collection<?> collection -> {
                // Convert arrays/lists like [2, 3, 4] to BsonArray with BsonValue elements
                BsonArray bsonArray = new BsonArray();
                for (Object element : collection) {
                    bsonArray.add(toBsonValue(element)); // Recursive conversion
                }
                yield bsonArray;
            }
            case Object[] array -> {
                // Convert Object arrays to BsonArray
                BsonArray bsonArray = new BsonArray();
                for (Object element : array) {
                    bsonArray.add(toBsonValue(element)); // Recursive conversion
                }
                yield bsonArray;
            }
            default ->
                    throw new IllegalArgumentException("Unsupported value type for BSON conversion: " + value.getClass().getSimpleName());
        };
    }

    /**
     * Converts a {@link BsonValue} to its corresponding Java object if it matches the desired type.
     * Returns null on type mismatch (strict type checking, no implicit conversions).
     *
     * @param value           the {@link BsonValue} to convert
     * @param desiredBsonType the required {@link BsonType}
     * @return the Java object, or null if types don't match
     * @throws IllegalArgumentException if the BSON type is unsupported
     */
    public static Object toObject(BsonValue value, BsonType desiredBsonType) {
        if (NumericWidening.isNumericBsonType(value.getBsonType())) {
            return NumericWidening.widenValue(value, desiredBsonType);
        }
        if (value.getBsonType() != desiredBsonType) {
            return null;
        }
        return switch (value.getBsonType()) {
            case STRING -> value.asString().getValue();
            case INT32 -> value.asInt32().getValue();
            case INT64 -> value.asInt64().getValue();
            case DOUBLE -> value.asDouble().getValue();
            case BOOLEAN -> value.asBoolean().getValue();
            case BINARY -> value.asBinary().getData();
            case DATE_TIME -> value.asDateTime().getValue();
            case TIMESTAMP -> value.asTimestamp().getValue();
            case DECIMAL128 -> value.asDecimal128().getValue().bigDecimalValue();
            case OBJECT_ID -> value.asObjectId().getValue();
            case NULL -> null;
            default -> throw new IllegalArgumentException("Unsupported BSON type: " + value.getBsonType());
        };
    }

    /**
     * Compares two {@link BsonType} values for strict equality.
     *
     * @param valueType   the actual {@link BsonType} being compared
     * @param desiredType the desired {@link BsonType} to compare against
     * @return {@code true} if the types are equal, {@code false} otherwise
     */
    public static boolean equals(BsonType valueType, BsonType desiredType) {
        return desiredType.equals(valueType);
    }

    /**
     * Compares two {@link BsonValue} instances and determines their relative order based on their types and values.
     * If the BSON type is not supported for comparison, an {@link IllegalArgumentException} is thrown.
     *
     * @param a the first {@link BsonValue} to compare
     * @param b the second {@link BsonValue} to compare
     * @return 0 if the values are equal, a negative integer if {@code a} is less than {@code b},
     * or a positive integer if {@code a} is greater than {@code b}
     * @throws IllegalArgumentException if the type of {@code a} or {@code b} is unsupported for comparison
     */
    public static int compareBsonValues(BsonValue a, BsonValue b) {
        if (a.equals(b)) {
            return 0;
        }
        return switch (a.getBsonType()) {
            case DOUBLE -> a.asDouble().compareTo(b.asDouble());
            case STRING -> a.asString().compareTo(b.asString());
            case BINARY -> Arrays.compare(a.asBinary().getData(), b.asBinary().getData());
            case BOOLEAN -> a.asBoolean().compareTo(b.asBoolean());
            case DATE_TIME -> a.asDateTime().compareTo(b.asDateTime());
            case NULL -> 0;
            case INT32 -> a.asInt32().compareTo(b.asInt32());
            case TIMESTAMP -> a.asTimestamp().compareTo(b.asTimestamp());
            case INT64 -> a.asInt64().compareTo(b.asInt64());
            case DECIMAL128 -> a.asDecimal128().getValue().compareTo(b.asDecimal128().getValue());
            case OBJECT_ID -> a.asObjectId().getValue().compareTo(b.asObjectId().getValue());
            default -> throw new IllegalArgumentException("Unsupported bson type for indexing");
        };
    }

    /**
     * Converts a BsonValue to its equivalent BqlValue representation.
     *
     * @param bsonValue the BsonValue to convert
     * @return the equivalent BqlValue, or null if the type is unsupported
     */
    public static BqlValue convertBsonValueToBqlValue(BsonValue bsonValue) {
        return switch (bsonValue.getBsonType()) {
            case STRING -> new StringVal(bsonValue.asString().getValue());
            case INT32 -> new Int32Val(bsonValue.asInt32().getValue());
            case INT64 -> new Int64Val(bsonValue.asInt64().getValue());
            case DOUBLE -> new DoubleVal(bsonValue.asDouble().getValue());
            case DECIMAL128 -> new Decimal128Val(bsonValue.asDecimal128().decimal128Value().bigDecimalValue());
            case BOOLEAN -> new BooleanVal(bsonValue.asBoolean().getValue());
            case NULL -> NullVal.INSTANCE;
            case DATE_TIME -> new DateTimeVal(bsonValue.asDateTime().getValue());
            case TIMESTAMP -> new TimestampVal(bsonValue.asTimestamp().getValue());
            case BINARY -> new BinaryVal(bsonValue.asBinary().getData());
            case OBJECT_ID -> new ObjectIdVal(bsonValue.asObjectId().getValue());
            case ARRAY -> {
                List<BqlValue> elements = new ArrayList<>();
                for (BsonValue element : bsonValue.asArray()) {
                    BqlValue converted = convertBsonValueToBqlValue(element);
                    if (converted != null) {
                        elements.add(converted);
                    }
                }
                yield new ArrayVal(elements);
            }
            case DOCUMENT -> {
                Map<String, BqlValue> fields = new LinkedHashMap<>();
                BsonDocument doc = bsonValue.asDocument();
                for (String key : doc.keySet()) {
                    BqlValue converted = convertBsonValueToBqlValue(doc.get(key));
                    if (converted != null) {
                        fields.put(key, converted);
                    }
                }
                yield new DocumentVal(fields);
            }
            default -> null;
        };
    }

    /**
     * Sets a value at a nested path in a BsonDocument, creating intermediate documents as needed.
     *
     * @param doc   the document to modify
     * @param path  dot-notation path (e.g., "address.city")
     * @param value the value to set
     */
    public static void setNestedField(BsonDocument doc, String path, BsonValue value) {
        String[] parts = StringUtil.split(path);
        BsonDocument current = doc;

        for (int i = 0; i < parts.length - 1; i++) {
            String part = parts[i];
            if (!current.containsKey(part)) {
                current.put(part, new BsonDocument());
            }
            BsonValue nested = current.get(part);
            if (nested instanceof BsonDocument nestedDoc) {
                current = nestedDoc;
            } else {
                // If the existing value is not a document, replace it with one
                BsonDocument newDoc = new BsonDocument();
                current.put(part, newDoc);
                current = newDoc;
            }
        }

        current.put(parts[parts.length - 1], value);
    }

    /**
     * Converts a Java object to its equivalent BqlValue representation.
     *
     * @param obj the Java object to convert (may include BsonValue types)
     * @return the equivalent BqlValue, or NullVal if obj is null
     * @throws IllegalArgumentException if the object type is not supported
     */
    public static BqlValue convertObjectToBqlValue(Object obj) {
        if (obj == null) {
            return NullVal.INSTANCE;
        }
        return switch (obj) {
            // Java primitive types
            case Integer i -> new Int32Val(i);
            case Long l -> new Int64Val(l);
            case Double d -> new DoubleVal(d);
            case String s -> new StringVal(s);
            case Boolean b -> new BooleanVal(b);
            case BigDecimal bd -> new Decimal128Val(bd);
            case byte[] bytes -> new BinaryVal(bytes);
            case Versionstamp vs -> new VersionstampVal(vs);
            // BSON types (passed through from UpdateOptionsConverter for type disambiguation)
            case BsonInt32 i32 -> new Int32Val(i32.getValue());
            case BsonInt64 i64 -> new Int64Val(i64.getValue());
            case BsonDouble dbl -> new DoubleVal(dbl.getValue());
            case BsonString str -> new StringVal(str.getValue());
            case BsonBoolean bool -> new BooleanVal(bool.getValue());
            case BsonDecimal128 dec -> new Decimal128Val(dec.decimal128Value().bigDecimalValue());
            case BsonNull ignored -> NullVal.INSTANCE;
            case BsonDateTime dt -> new DateTimeVal(dt.getValue());
            case BsonTimestamp ts -> new TimestampVal(ts.getValue());
            case BsonBinary bin -> new BinaryVal(bin.getData());
            case BsonArray arr -> convertBsonValueToBqlValue(arr);
            case BsonDocument doc -> convertBsonValueToBqlValue(doc);
            case ObjectId objectId -> new ObjectIdVal(objectId);
            default -> throw new IllegalArgumentException("Unsupported operand type: " + obj.getClass());
        };
    }

    /**
     * Converts a BqlValue to its equivalent BsonValue.
     */
    public static BsonValue bqlValueToBsonValue(BqlValue value) {
        return switch (value) {
            case StringVal v -> new BsonString(v.value());
            case Int32Val v -> new BsonInt32(v.value());
            case Int64Val v -> new BsonInt64(v.value());
            case DoubleVal v -> new BsonDouble(v.value());
            case BooleanVal v -> new BsonBoolean(v.value());
            case NullVal ignored -> BsonNull.VALUE;
            case Decimal128Val v -> new BsonDecimal128(new Decimal128(v.value()));
            case DateTimeVal v -> new BsonDateTime(v.value());
            case TimestampVal v -> new BsonTimestamp(v.value());
            case BinaryVal v -> new BsonBinary(v.value());
            case ArrayVal v -> {
                BsonArray arr = new BsonArray();
                for (BqlValue element : v.values()) {
                    BsonValue converted = bqlValueToBsonValue(element);
                    if (converted != null) {
                        arr.add(converted);
                    }
                }
                yield arr;
            }
            case DocumentVal v -> {
                BsonDocument nested = new BsonDocument();
                for (Map.Entry<String, BqlValue> entry : v.fields().entrySet()) {
                    BsonValue converted = bqlValueToBsonValue(entry.getValue());
                    if (converted != null) {
                        nested.put(entry.getKey(), converted);
                    }
                }
                yield nested;
            }
            case VersionstampVal v -> new BsonBinary(v.value().getBytes());
            case ObjectIdVal v -> new BsonObjectId(v.value());
            // A $regex operand is a matcher, never a storable document value.
            case RegexVal ignored -> throw new UnsupportedOperationException("$regex cannot be converted to a BSON value");
        };
    }

    public static String bsonTypeToString(BsonType type) {
        return switch (type) {
            case BsonType.DOUBLE -> "Double";
            case BsonType.STRING -> "String";
            case BsonType.BINARY -> "Binary";
            case BsonType.BOOLEAN -> "Boolean";
            case BsonType.DATE_TIME -> "DateTime";
            case BsonType.INT32 -> "Int32";
            case BsonType.INT64 -> "Int64";
            case BsonType.TIMESTAMP -> "Timestamp";
            case BsonType.DECIMAL128 -> "Decimal128";
            case BsonType.OBJECT_ID -> "ObjectId";
            default -> throw new IllegalArgumentException("Unsupported BSON type: " + type);
        };
    }
}