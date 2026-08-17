/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume.replication;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationHintTest {

    @Test
    void shouldEncodeAndDecodeHint() {
        // Behavior: encode then decode returns the same hint
        ReplicationHint hint = new ReplicationHint(HintType.VECTOR, 42L, 1024L);
        byte[] encoded = hint.encode();

        assertEquals(ReplicationHint.ENCODED_SIZE, encoded.length);
        assertEquals(hint, ReplicationHint.decode(encoded));
    }

    @Test
    void shouldEncodeAndDecodeMaxLongValues() {
        // Behavior: a hint with Long.MAX_VALUE segmentId and position survives a round trip
        ReplicationHint hint = new ReplicationHint(HintType.VECTOR, Long.MAX_VALUE, Long.MAX_VALUE);

        assertEquals(hint, ReplicationHint.decode(hint.encode()));
    }

    @Test
    void shouldExtractTypeWithoutFullDecode() {
        // Behavior: extractType returns the hint type without decoding the whole hint
        ReplicationHint hint = new ReplicationHint(HintType.VECTOR, 7L, 13L);

        assertEquals(HintType.VECTOR, ReplicationHint.extractType(hint.encode()));
    }

    @Test
    void shouldThrowWhenSizeIsInvalid() {
        // Behavior: decode and extractType reject arrays that are shorter or longer than the encoded size
        byte[] shorter = new byte[ReplicationHint.ENCODED_SIZE - 1];
        byte[] longer = new byte[ReplicationHint.ENCODED_SIZE + 1];

        assertThrows(IllegalArgumentException.class, () -> ReplicationHint.decode(shorter));
        assertThrows(IllegalArgumentException.class, () -> ReplicationHint.extractType(shorter));
        assertThrows(IllegalArgumentException.class, () -> ReplicationHint.decode(longer));
        assertThrows(IllegalArgumentException.class, () -> ReplicationHint.extractType(longer));
    }

    @Test
    void shouldThrowWhenTypeIsUnknown() {
        // Behavior: decode rejects an unknown hint type
        byte[] encoded = new ReplicationHint(HintType.VECTOR, 1L, 2L).encode();
        byte[] corrupted = Arrays.copyOf(encoded, encoded.length);
        corrupted[0] = (byte) 0x7F;

        assertThrows(IllegalArgumentException.class, () -> ReplicationHint.decode(corrupted));
    }
}
