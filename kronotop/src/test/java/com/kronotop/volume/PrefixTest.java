/*
 * Copyright (c) 2023-2025 Kronotop
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

public class PrefixTest {
    @Test
    public void test_Prefix() {
        Prefix prefix = new Prefix("test");

        assertNotNull(prefix.asBytes());
        assertTrue(prefix.asLong() > 0);
        assertTrue(prefix.hashCode() > 0);
    }

    @Test
    public void test_fromBytes() {
        Prefix prefix = new Prefix("test");
        assertEquals(prefix, Prefix.fromBytes(prefix.asBytes()));
    }
}
