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

package com.kronotop.cluster.client.protocol;

import io.lettuce.core.codec.StringCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ChangeLogRangeOutputTest {

    private ChangeLogRangeOutput<String, String> output;

    @BeforeEach
    void setUp() {
        output = new ChangeLogRangeOutput<>(StringCodec.UTF8);
    }

    private ByteBuffer toBuffer(String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldDecodeEmptyArray() {
        output.multiArray(0);
        output.complete(0);

        List<ChangeLogEntryResponse> result = output.get();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldDecodeSingleEntryWithAfterOnly() {
        // Simulate APPEND entry (has after, no before)
        output.multiArray(1);

        output.multiMap(4);
        output.setSingle(toBuffer("versionstamp"));
        output.setSingle(toBuffer("ABC123"));
        output.setSingle(toBuffer("kind"));
        output.setSingle(toBuffer("APPEND"));
        output.setSingle(toBuffer("prefix"));
        output.set(1L);
        output.setSingle(toBuffer("after"));

        output.multiMap(4);
        output.setSingle(toBuffer("sequence_number"));
        output.set(100L);
        output.setSingle(toBuffer("segment_id"));
        output.set(0L);
        output.setSingle(toBuffer("position"));
        output.set(0L);
        output.setSingle(toBuffer("length"));
        output.set(256L);
        output.complete(2);

        output.complete(1);
        output.complete(0);

        List<ChangeLogEntryResponse> result = output.get();
        assertEquals(1, result.size());

        ChangeLogEntryResponse entry = result.get(0);
        assertEquals("ABC123", entry.versionstamp());
        assertEquals("APPEND", entry.kind());
        assertEquals(1L, entry.prefix());
        assertFalse(entry.hasBefore());
        assertTrue(entry.hasAfter());

        ChangeLogCoordinateResponse after = entry.after();
        assertNotNull(after);
        assertEquals(100L, after.sequenceNumber());
        assertEquals(0L, after.segmentId());
        assertEquals(0L, after.position());
        assertEquals(256L, after.length());
    }

    @Test
    void shouldDecodeSingleEntryWithBeforeOnly() {
        // Simulate DELETE entry (has before, no after)
        output.multiArray(1);

        output.multiMap(4);
        output.setSingle(toBuffer("versionstamp"));
        output.setSingle(toBuffer("DEF456"));
        output.setSingle(toBuffer("kind"));
        output.setSingle(toBuffer("DELETE"));
        output.setSingle(toBuffer("prefix"));
        output.set(2L);
        output.setSingle(toBuffer("before"));

        output.multiMap(4);
        output.setSingle(toBuffer("sequence_number"));
        output.set(50L);
        output.setSingle(toBuffer("segment_id"));
        output.set(1L);
        output.setSingle(toBuffer("position"));
        output.set(512L);
        output.setSingle(toBuffer("length"));
        output.set(128L);
        output.complete(2);

        output.complete(1);
        output.complete(0);

        List<ChangeLogEntryResponse> result = output.get();
        assertEquals(1, result.size());

        ChangeLogEntryResponse entry = result.get(0);
        assertEquals("DEF456", entry.versionstamp());
        assertEquals("DELETE", entry.kind());
        assertEquals(2L, entry.prefix());
        assertTrue(entry.hasBefore());
        assertFalse(entry.hasAfter());

        ChangeLogCoordinateResponse before = entry.before();
        assertNotNull(before);
        assertEquals(50L, before.sequenceNumber());
        assertEquals(1L, before.segmentId());
        assertEquals(512L, before.position());
        assertEquals(128L, before.length());
    }

    @Test
    void shouldDecodeSingleEntryWithBothBeforeAndAfter() {
        // Simulate UPDATE entry (has both before and after)
        output.multiArray(1);

        output.multiMap(5);
        output.setSingle(toBuffer("versionstamp"));
        output.setSingle(toBuffer("GHI789"));
        output.setSingle(toBuffer("kind"));
        output.setSingle(toBuffer("UPDATE"));
        output.setSingle(toBuffer("prefix"));
        output.set(3L);

        // before
        output.setSingle(toBuffer("before"));
        output.multiMap(4);
        output.setSingle(toBuffer("sequence_number"));
        output.set(10L);
        output.setSingle(toBuffer("segment_id"));
        output.set(0L);
        output.setSingle(toBuffer("position"));
        output.set(100L);
        output.setSingle(toBuffer("length"));
        output.set(50L);
        output.complete(2);

        // after
        output.setSingle(toBuffer("after"));
        output.multiMap(4);
        output.setSingle(toBuffer("sequence_number"));
        output.set(20L);
        output.setSingle(toBuffer("segment_id"));
        output.set(1L);
        output.setSingle(toBuffer("position"));
        output.set(200L);
        output.setSingle(toBuffer("length"));
        output.set(75L);
        output.complete(2);

        output.complete(1);
        output.complete(0);

        List<ChangeLogEntryResponse> result = output.get();
        assertEquals(1, result.size());

        ChangeLogEntryResponse entry = result.get(0);
        assertEquals("GHI789", entry.versionstamp());
        assertEquals("UPDATE", entry.kind());
        assertEquals(3L, entry.prefix());
        assertTrue(entry.hasBefore());
        assertTrue(entry.hasAfter());

        assertEquals(10L, entry.before().sequenceNumber());
        assertEquals(20L, entry.after().sequenceNumber());
    }

    @Test
    void shouldDecodeMultipleEntries() {
        output.multiArray(2);

        // First entry (APPEND)
        output.multiMap(4);
        output.setSingle(toBuffer("versionstamp"));
        output.setSingle(toBuffer("ENTRY1"));
        output.setSingle(toBuffer("kind"));
        output.setSingle(toBuffer("APPEND"));
        output.setSingle(toBuffer("prefix"));
        output.set(1L);
        output.setSingle(toBuffer("after"));
        output.multiMap(4);
        output.setSingle(toBuffer("sequence_number"));
        output.set(100L);
        output.setSingle(toBuffer("segment_id"));
        output.set(0L);
        output.setSingle(toBuffer("position"));
        output.set(0L);
        output.setSingle(toBuffer("length"));
        output.set(100L);
        output.complete(2);
        output.complete(1);

        // Second entry (DELETE)
        output.multiMap(4);
        output.setSingle(toBuffer("versionstamp"));
        output.setSingle(toBuffer("ENTRY2"));
        output.setSingle(toBuffer("kind"));
        output.setSingle(toBuffer("DELETE"));
        output.setSingle(toBuffer("prefix"));
        output.set(2L);
        output.setSingle(toBuffer("before"));
        output.multiMap(4);
        output.setSingle(toBuffer("sequence_number"));
        output.set(200L);
        output.setSingle(toBuffer("segment_id"));
        output.set(1L);
        output.setSingle(toBuffer("position"));
        output.set(50L);
        output.setSingle(toBuffer("length"));
        output.set(75L);
        output.complete(2);
        output.complete(1);

        output.complete(0);

        List<ChangeLogEntryResponse> result = output.get();
        assertEquals(2, result.size());

        assertEquals("ENTRY1", result.get(0).versionstamp());
        assertEquals("APPEND", result.get(0).kind());

        assertEquals("ENTRY2", result.get(1).versionstamp());
        assertEquals("DELETE", result.get(1).kind());
    }
}
