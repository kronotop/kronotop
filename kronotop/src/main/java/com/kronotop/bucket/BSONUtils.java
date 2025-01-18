// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.nio.ByteBuffer;

/**
 * Utility class for handling BSON {@code Document} serialization and deserialization.
 * Provides methods for converting BSON {@code Document} objects into byte arrays and vice versa.
 * <p>
 * This class uses a static {@code DocumentCodec} instance to encode and decode BSON {@code Document} objects.
 * It operates with the BSON binary serialization format for efficient storage and transmission.
 */
public class BSONUtils {

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
}