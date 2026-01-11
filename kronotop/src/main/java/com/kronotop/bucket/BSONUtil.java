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

package com.kronotop.bucket;

import com.kronotop.bucket.bson.BasicOutputBuffer;
import org.bson.*;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonReader;
import org.bson.types.Binary;
import org.bson.types.Decimal128;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;

/**
 * Utility class for BSON document serialization and deserialization.
 * Provides methods for converting {@code BsonDocument} objects to byte arrays and vice versa.
 * <p>
 * Uses {@code BsonDocumentCodec} for encoding and decoding operations.
 */
public class BSONUtil {

    private static final Codec<BsonDocument> BSON_DOCUMENT_CODEC = new BsonDocumentCodec();

    /**
     * Serializes a {@code BsonDocument} to a BSON-encoded byte array.
     *
     * @param document the document to serialize
     * @return the serialized BSON data as a byte array
     */
    public static byte[] toBytes(BsonDocument document) {
       return toByteBuffer(document).array();
    }

    /**
     * Serializes a {@code BsonDocument} to a BSON-encoded {@code ByteBuffer}.
     *
     * @param document the document to serialize
     * @return the serialized BSON data as a {@code ByteBuffer} ready for reading
     */
    public static ByteBuffer toByteBuffer(BsonDocument document) {
        try (BasicOutputBuffer buffer = new BasicOutputBuffer()) {
            BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
            BSON_DOCUMENT_CODEC.encode(writer, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
            return buffer.getInternalBuffer();
        }
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
     *
     * @param bytes the JSON-encoded byte array
     * @return the decoded {@code BsonDocument}
     */
    public static BsonDocument fromJson(byte[] bytes) {
        Reader targetReader = new InputStreamReader(new ByteArrayInputStream(bytes));
        try (JsonReader reader = new JsonReader(targetReader)) {
            return BSON_DOCUMENT_CODEC.decode(reader, DecoderContext.builder().build());
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
        return BSONUtil.toBytes(RawBsonDocument.parse(data));
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
            case Binary binaryVal -> new BsonBinary(binaryVal.getData()); // Convert org.bson.types.Binary to BsonBinary
            case BsonValue bsonVal -> bsonVal; // Already a BsonValue
            case Document docVal -> docVal.toBsonDocument(); // Convert Document to BsonDocument
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
            case NULL -> null;
            default -> {
                throw new IllegalArgumentException("Unsupported BSON type: " + value.getBsonType());
            }
        };
    }

    /**
     * Compares two {@link BsonType} values and determines if they are equivalent,
     * considering specific cases where one type is allowed to match another.
     * For example, a {@code valueType} of {@link BsonType#INT64} is considered
     * equivalent to a {@link BsonType#INT32}.
     *
     * @param valueType   the actual {@link BsonType} being compared
     * @param desiredType the desired {@link BsonType} to compare against
     * @return {@code true} if the types are considered equivalent, {@code false} otherwise
     */
    public static boolean equals(BsonType valueType, BsonType desiredType) {
        if (desiredType.equals(BsonType.INT32)) {
            if (valueType.equals(BsonType.INT64)) {
                return true;
            }
        }
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
            default -> throw new IllegalArgumentException("Unsupported bson type for indexing");
        };
    }
}