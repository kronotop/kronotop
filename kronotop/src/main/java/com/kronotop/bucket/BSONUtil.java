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

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonReader;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;

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
}