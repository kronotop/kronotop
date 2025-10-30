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

import org.bson.*;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
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

import static org.bson.BsonType.INT32;

/**
 * Utility class for handling BSON {@code Document} serialization and deserialization.
 * Provides methods for converting BSON {@code Document} objects into byte arrays and vice versa.
 * <p>
 * This class uses a static {@code DocumentCodec} instance to encode and decode BSON {@code Document} objects.
 * It operates with the BSON binary serialization format for efficient storage and transmission.
 */
public class BSONUtil {

    private static final Codec<Document> DOCUMENT_CODEC = new DocumentCodec();

    /**
     * Converts a BSON {@code Document} into its byte array representation.
     *
     * @param document the {@code Document} to be converted into a byte array
     * @return a byte array representing the serialized BSON document
     */
    public static byte[] toBytes(Document document) {
        try (BasicOutputBuffer buffer = new BasicOutputBuffer()) {
            BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
            DOCUMENT_CODEC.encode(writer, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
            return buffer.toByteArray();
        }
    }

    /**
     * Converts a byte array representation of a BSON document into its Document object form.
     *
     * @param bytes the byte array containing the serialized BSON document
     * @return the decoded {@code Document} object
     */
    public static Document toDocument(byte[] bytes) {
        try (BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes))) {
            return DOCUMENT_CODEC.decode(reader, DecoderContext.builder().build());
        }
    }

    /**
     * Converts a JSON-encoded byte array into a BSON {@code Document}.
     *
     * @param bytes the JSON-encoded byte array to be converted into a {@code Document}
     * @return the BSON {@code Document} decoded from the provided byte array
     */
    public static Document fromJson(byte[] bytes) {
        Reader targetReader = new InputStreamReader(new ByteArrayInputStream(bytes));
        try (JsonReader reader = new JsonReader(targetReader)) {
            return DOCUMENT_CODEC.decode(reader, DecoderContext.builder().build());
        }
    }

    /**
     * Deserializes a byte array containing BSON data into a {@code Document}.
     *
     * @param bytes the byte array containing the serialized BSON data
     * @return the deserialized {@code Document} object
     */
    public static Document fromBson(byte[] bytes) {
        return fromBson(ByteBuffer.wrap(bytes));
    }

    /**
     * Deserializes a {@code ByteBuffer} containing BSON data into a {@code Document}.
     *
     * @param buffer the {@code ByteBuffer} containing the serialized BSON data
     * @return the deserialized {@code Document} object
     */
    public static Document fromBson(ByteBuffer buffer) {
        try (BsonBinaryReader reader = new BsonBinaryReader(buffer)) {
            return DOCUMENT_CODEC.decode(reader, DecoderContext.builder().build());
        }
    }

    /**
     * Converts a JSON string into a BSON {@code Document} and then serializes it to a byte array.
     *
     * @param data the JSON string to be parsed and converted into a BSON {@code Document}
     * @return a byte array representing the serialized BSON document
     */
    public static byte[] jsonToDocumentThenBytes(String data) {
        return BSONUtil.toBytes(Document.parse(data));
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
     * Converts a given {@link BsonValue} to its corresponding Java object based on the BSON type.
     * If there is a mismatch between the actual BSON type of the value and the expected BSON type,
     * null is returned unless the mismatch is specifically allowed (e.g., INT32 values are allowed
     * where INT64 is expected).
     *
     * @param value           the {@link BsonValue} to be converted
     * @param desiredBsonType the expected {@link BsonType} of the {@code value}
     * @return a Java object that corresponds to the BSON type of the {@code value},
     * or null if there is a type mismatch or the BSON type is unsupported
     * @throws IllegalArgumentException if an unsupported BSON type is encountered
     */
    public static Object toObject(BsonValue value, BsonType desiredBsonType) {
        // Check if the actual BSON type matches the expected type from IndexDefinition
        if (value.getBsonType() != desiredBsonType) {
            // Int64 covers Int32 values
            if (!(desiredBsonType.equals(BsonType.INT64) && value.getBsonType().equals(INT32))) {
                // Type mismatches are not indexed, but documents are still persisted.
                return null;
            }
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
     *         or a positive integer if {@code a} is greater than {@code b}
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