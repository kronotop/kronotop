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

package com.kronotop.volume.segment;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

public class SegmentMetadataTest {

    @Test
    public void decodeTest_validByteBuffer_decodesCorrectly() {

        // Given
        long expectedId = 1L;
        long expectedSize = 2L;
        long expectedPosition = 3L;

        ByteBuffer buffer = ByteBuffer.allocate(SegmentMetadata.HEADER_SIZE);
        buffer.putLong(expectedId);
        buffer.putLong(expectedSize);
        buffer.putLong(expectedPosition);

        // When
        SegmentMetadata segmentMetadata = SegmentMetadata.decode(buffer);

        // Then
        assertNotNull(segmentMetadata);
        assertEquals(expectedId, segmentMetadata.getId());
        assertEquals(expectedSize, segmentMetadata.getSize());
        assertEquals(expectedPosition, segmentMetadata.getPosition());
    }

    @Test
    public void decodeTest_invalidByteBuffer_throwsException() {

        // Given
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(1L);
        buffer.putLong(2L);
        buffer.flip();

        // When / Then
        assertThrows(Exception.class, () -> SegmentMetadata.decode(buffer));
    }

    @Test
    public void encodeTest_validSegmentMetadata_encodesCorrectly() {
        // Given
        long expectedId = 1L;
        long expectedSize = 2L;
        long expectedPosition = 3L;

        SegmentMetadata segmentMetadata = new SegmentMetadata(expectedId, expectedSize);
        segmentMetadata.setPosition(expectedPosition);

        // When
        ByteBuffer buffer = segmentMetadata.encode();

        // Then
        assertNotNull(buffer);
        assertEquals(expectedId, buffer.getLong());
        assertEquals(expectedSize, buffer.getLong());
        assertEquals(expectedPosition, buffer.getLong());
    }
}
