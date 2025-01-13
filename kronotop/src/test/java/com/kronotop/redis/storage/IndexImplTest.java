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

package com.kronotop.redis.storage;

import com.kronotop.redis.storage.index.Index;
import com.kronotop.redis.storage.index.Projection;
import com.kronotop.redis.storage.index.impl.IndexImpl;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class IndexImplTest {

    private void checkProjection(Index index) {
        Projection first = index.getProjection(0, 10);
        assertEquals(first.getKeys().size(), 10);

        for (int i = 0; i < 10; i++) {
            String key = String.format("key-{%d}", i);
            assertEquals(first.getKeys().get(i), key);
        }

        Projection second = index.getProjection(first.getCursor(), 10);
        for (int i = 10; i < 20; i++) {
            String key = String.format("key-{%d}", i);
            assertEquals(second.getKeys().get(i - 10), key);
        }

        assertEquals(0, second.getCursor());
    }

    @Test
    public void testGetProjection_buffer() {
        Index index = new IndexImpl(0);
        for (int i = 0; i < 20; i++) {
            index.add(String.format("key-{%d}", i));
        }
        checkProjection(index);
    }

    @Test
    public void testGetProjection_index() {
        Index index = new IndexImpl(123);
        for (int i = 0; i < 20; i++) {
            index.add(String.format("key-{%d}", i));
        }
        index.flush();
        checkProjection(index);
    }

    @Test
    public void testGetProjection_mixed() {
        Index index = new IndexImpl(0);
        for (int i = 0; i < 10; i++) {
            index.add(String.format("key-{%d}", i));
        }
        index.flush();

        for (int i = 10; i < 20; i++) {
            index.add(String.format("key-{%d}", i));
        }

        checkProjection(index);
    }

    @Test
    public void testHead_index() {
        Index index = new IndexImpl(10);
        for (int i = 0; i < 10; i++) {
            index.add(String.format("key-{%d}", i));
        }
        index.flush();
        assertTrue(index.head() > 0);
    }

    @Test
    public void testHead_buffer() {
        Index index = new IndexImpl(10);
        for (int i = 0; i < 10; i++) {
            index.add(String.format("key-{%d}", i));
        }
        assertTrue(index.head() > 0);
    }

    @Test
    public void testHead_random_from_index() {
        Index index = new IndexImpl(100);
        for (int i = 0; i < 10; i++) {
            index.add(String.format("key-{%d}", i));
        }
        index.flush();
        assertDoesNotThrow(index::random);
    }

    @Test
    public void testHead_random_from_buffer() {
        Index index = new IndexImpl(100);
        for (int i = 0; i < 10; i++) {
            index.add(String.format("key-{%d}", i));
        }
        assertDoesNotThrow(index::random);
    }

    @Test
    public void testGetProjection_remove() {
        Index index = new IndexImpl(0);
        for (int i = 0; i < 10; i++) {
            index.add(String.format("key-{%d}", i));
        }
        index.flush();

        for (int i = 10; i < 20; i++) {
            index.add(String.format("key-{%d}", i));
        }

        for (int i = 0; i < 20; i++) {
            if (i % 2 == 0) {
                index.remove(String.format("key-{%d}", i));
            }
        }

        List<String> expectedKeys = Arrays.asList(
                "key-{1}", "key-{3}", "key-{5}", "key-{7}",
                "key-{9}", "key-{11}", "key-{13}",
                "key-{15}", "key-{17}", "key-{19}");

        Projection projection = index.getProjection(0, 20);
        assertEquals(0, projection.getCursor());
        assertEquals(expectedKeys, projection.getKeys());
    }
}
