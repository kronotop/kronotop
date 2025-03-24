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

package com.kronotop.internal;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RoundRobinTest {

    @Test
    void test_next() {
        List<Integer> elements = List.of(0, 1, 2, 3, 4, 5, 6);
        RoundRobin<Integer> rr = new RoundRobin<>(elements);

        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < elements.size(); i++) {
            result.add(rr.next());
        }
        assertEquals(elements, result);
    }

    @Test
    void test_add_then_next() {
        List<Integer> elements = List.of(0, 1, 2, 3, 4, 5, 6);
        RoundRobin<Integer> rr = new RoundRobin<>(elements);

        for (int i = 0; i < elements.size(); i++) {
            rr.next(); // consume all elements
        }
        rr.add(20);
        assertEquals(20, rr.next());
        assertEquals(0, rr.next());
    }

    @Test
    void test_remove_then_next() {
        List<Integer> elements = List.of(0, 1, 2, 3, 4, 5, 6);
        RoundRobin<Integer> rr = new RoundRobin<>(elements);

        for (int i = 0; i < elements.size(); i++) {
            rr.next(); // consume all elements
        }
        rr.remove(0);
        assertNotEquals(0, rr.next());
    }
}