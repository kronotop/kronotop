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

package com.kronotop.volume;

import com.kronotop.volume.segment.Segment;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class EntryMetadataTest {

    @Test
    void decode_should_return_corresponding_EntryMetadata() {
        Prefix prefix = new Prefix("test");
        // Initialize necessary data
        String segment = Segment.generateName(10);
        long position = 1L;
        long length = 1L;
        ByteBuffer buffer = ByteBuffer.allocate(EntryMetadata.SIZE); // Including space for position and length
        buffer.put(segment.getBytes()).put(EntryMetadata.SUBSPACE_SEPARATOR).put(prefix.asBytes()).putLong(position).putLong(length).putInt(10).flip();

        // Invoke method on test
        EntryMetadata result = EntryMetadata.decode(buffer);

        // Check that the result has the same values
        assertEquals(segment, result.segment());
        assertEquals(prefix, Prefix.fromBytes(result.prefix()));
        assertEquals(position, result.position());
        assertEquals(length, result.length());
        assertEquals(10, result.id());
    }

    @Test
    void encode_should_return_corresponding_byte_buffer() {
        Prefix prefix = new Prefix("test");

        // Initialize necessary data
        int segmentId = 10;
        String segment = Segment.generateName(segmentId);
        long position = 1L;
        long length = 1L;
        int id = EntryMetadataIdGenerator.generate(1, segmentId, position);

        // Create EntryMetadata instance
        EntryMetadata entry = new EntryMetadata(segment, prefix.asBytes(), position, length, id);

        // Invoke method on test
        ByteBuffer result = entry.encode();

        EntryMetadata decoded = EntryMetadata.decode(result);
        assertThat(entry).usingRecursiveComparison().isEqualTo(decoded);
    }

    @Test
    void should_extract_id_from_encoded_entry_metadata() {
        Prefix prefix = new Prefix("test");

        // Initialize necessary data
        int segmentId = 10;
        String segment = Segment.generateName(segmentId);
        long position = 1L;
        long length = 1L;
        int id = EntryMetadataIdGenerator.generate(1, segmentId, position);

        // Create EntryMetadata instance
        EntryMetadata entry = new EntryMetadata(segment, prefix.asBytes(), position, length, id);

        // Invoke method on test
        ByteBuffer result = entry.encode();

        assertEquals(id, EntryMetadata.extractId(result));

        // Rewind works?
        EntryMetadata decoded = EntryMetadata.decode(result);
        assertThat(entry).usingRecursiveComparison().isEqualTo(decoded);
    }
}
