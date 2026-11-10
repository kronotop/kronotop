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

package com.kronotop.volume;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VolumeNamesTest {

    @Test
    void shouldFormatVolumeName() {
        // Behavior: formatting shard id 5 returns the volume name "shard-5".
        String name = VolumeNames.format(5);
        assertEquals("shard-5", name);
    }

    @Test
    void shouldParseVolumeName() {
        // Behavior: parsing the volume name "shard-10" returns shard id 10.
        int shardId = VolumeNames.parse("shard-10");
        assertEquals(10, shardId);
    }

    @Test
    void shouldThrowWhenParsingNull() {
        // Behavior: parsing a null name throws IllegalArgumentException with "name cannot be null".
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> VolumeNames.parse(null)
        );
        assertEquals("name cannot be null", exception.getMessage());
    }

    @Test
    void shouldThrowWhenParsingNameWithoutShardPrefix() {
        // Behavior: parsing a name that does not start with "shard-" throws IllegalArgumentException
        // with "invalid volume name: <name>".
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> VolumeNames.parse("volume-0")
        );
        assertEquals("invalid volume name: volume-0", exception.getMessage());
    }

    @Test
    void shouldThrowWhenParsingInvalidShardId() {
        // Behavior: parsing "shard-abc", where the shard id is not an integer, throws
        // IllegalArgumentException with "invalid shard id: abc".
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> VolumeNames.parse("shard-abc")
        );
        assertEquals("invalid shard id: abc", exception.getMessage());
    }

    @Test
    void shouldRoundTripFormatAndParse() {
        // Behavior: parsing the name formatted for shard id 42 returns shard id 42.
        int shardId = VolumeNames.parse(VolumeNames.format(42));
        assertEquals(42, shardId);
    }
}
