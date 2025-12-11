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

import com.kronotop.cluster.sharding.ShardKind;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VolumeNamesTest {

    @Test
    void shouldFormatRedisVolumeName() {
        String name = VolumeNames.format(ShardKind.REDIS, 0);
        assertEquals("redis-shard-0", name);
    }

    @Test
    void shouldFormatBucketVolumeName() {
        String name = VolumeNames.format(ShardKind.BUCKET, 5);
        assertEquals("bucket-shard-5", name);
    }

    @Test
    void shouldParseRedisVolumeName() {
        VolumeNames.Parsed parsed = VolumeNames.parse("redis-shard-0");
        assertEquals(ShardKind.REDIS, parsed.shardKind());
        assertEquals(0, parsed.shardId());
    }

    @Test
    void shouldParseBucketVolumeName() {
        VolumeNames.Parsed parsed = VolumeNames.parse("bucket-shard-10");
        assertEquals(ShardKind.BUCKET, parsed.shardKind());
        assertEquals(10, parsed.shardId());
    }

    @Test
    void shouldThrowWhenParsingNull() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> VolumeNames.parse(null)
        );
        assertEquals("name cannot be null", exception.getMessage());
    }

    @Test
    void shouldThrowWhenParsingInvalidFormat() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> VolumeNames.parse("invalid-name")
        );
        assertEquals("invalid volume name: invalid-name", exception.getMessage());
    }

    @Test
    void shouldThrowWhenParsingMissingShardKeyword() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> VolumeNames.parse("redis-volume-0")
        );
        assertEquals("invalid volume name: redis-volume-0", exception.getMessage());
    }

    @Test
    void shouldThrowWhenParsingInvalidShardKind() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> VolumeNames.parse("unknown-shard-0")
        );
        assertTrue(exception.getMessage().contains("invalid shard kind: unknown"));
    }

    @Test
    void shouldThrowWhenParsingInvalidShardId() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> VolumeNames.parse("redis-shard-abc")
        );
        assertTrue(exception.getMessage().contains("invalid shard id: abc"));
    }

    @Test
    void shouldRoundTripFormatAndParse() {
        for (ShardKind kind : ShardKind.values()) {
            String name = VolumeNames.format(kind, 42);
            VolumeNames.Parsed parsed = VolumeNames.parse(name);
            assertEquals(kind, parsed.shardKind());
            assertEquals(42, parsed.shardId());
        }
    }
}
