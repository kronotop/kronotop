/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.redis.storage.persistence;

import com.kronotop.redis.string.StringValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

public class StringPackTest {

    @Test
    public void test_pack_unpack() throws IOException {
        StringPack stringPack = new StringPack("foobar", new StringValue("barfoo".getBytes(), 1000L));
        ByteBuffer data = stringPack.pack();

        StringPack unpacked = StringPack.unpack(data);

        assertEquals(stringPack.key(), unpacked.key());
        assertArrayEquals(stringPack.stringValue().value(), unpacked.stringValue().value());
        assertEquals(stringPack.stringValue().ttl(), unpacked.stringValue().ttl());
    }

    @Test
    public void test_unpack_zero_length_array() throws IOException {
        ByteBuffer data = ByteBuffer.allocate(0);
        assertThrows(IOException.class, () -> StringPack.unpack(data));
    }
}