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


package com.kronotop.redis.storage.syncer;

import com.kronotop.redis.handlers.hash.HashFieldValue;
import com.kronotop.redis.storage.HashFieldPack;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

public class HashFieldPackTest {

    @Test
    public void test_pack_then_unpack() {
        HashFieldPack actual = new HashFieldPack("key", "field", new HashFieldValue("foobar".getBytes(), 1000));
        ByteBuffer buffer = actual.pack();

        HashFieldPack expected = assertDoesNotThrow(() -> HashFieldPack.unpack(buffer));
        assertEquals(actual.key(), expected.key());
        assertEquals(actual.field(), expected.field());
        assertEquals(actual.hashFieldValue().ttl(), expected.hashFieldValue().ttl());
        assertArrayEquals(actual.hashFieldValue().value(), expected.hashFieldValue().value());
    }
}