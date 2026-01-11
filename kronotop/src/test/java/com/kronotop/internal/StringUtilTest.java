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

package com.kronotop.internal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StringUtilTest {

    @Test
    void shouldSplitSingleSegment() {
        String[] result = StringUtil.split("field");
        assertArrayEquals(new String[]{"field"}, result);
    }

    @Test
    void shouldSplitTwoSegments() {
        String[] result = StringUtil.split("user.name");
        assertArrayEquals(new String[]{"user", "name"}, result);
    }

    @Test
    void shouldSplitDeeplyNestedPath() {
        String[] result = StringUtil.split("a.b.c.d.e.f.g");
        assertArrayEquals(new String[]{"a", "b", "c", "d", "e", "f", "g"}, result);
    }

    @Test
    void shouldHandleArrayIndex() {
        String[] result = StringUtil.split("items.0");
        assertArrayEquals(new String[]{"items", "0"}, result);
    }

    @Test
    void shouldHandleComplexPathWithArrayIndex() {
        String[] result = StringUtil.split("users.0.profile.name");
        assertArrayEquals(new String[]{"users", "0", "profile", "name"}, result);
    }

    @Test
    void shouldHandleEmptyString() {
        String[] result = StringUtil.split("");
        assertArrayEquals(new String[]{""}, result);
    }

    @Test
    void shouldHandleTrailingDot() {
        String[] result = StringUtil.split("field.");
        assertArrayEquals(new String[]{"field", ""}, result);
    }

    @Test
    void shouldHandleLeadingDot() {
        String[] result = StringUtil.split(".field");
        assertArrayEquals(new String[]{"", "field"}, result);
    }

    @Test
    void shouldHandleConsecutiveDots() {
        String[] result = StringUtil.split("a..b");
        assertArrayEquals(new String[]{"a", "", "b"}, result);
    }

    @Test
    void shouldHandleOnlyDot() {
        String[] result = StringUtil.split(".");
        assertArrayEquals(new String[]{"", ""}, result);
    }

    @Test
    void shouldHandleMultipleDots() {
        String[] result = StringUtil.split("...");
        assertArrayEquals(new String[]{"", "", "", ""}, result);
    }

    @Test
    void shouldHandleNumericFieldNames() {
        String[] result = StringUtil.split("123.456.789");
        assertArrayEquals(new String[]{"123", "456", "789"}, result);
    }

    @Test
    void shouldHandleSpecialCharactersInFieldNames() {
        String[] result = StringUtil.split("field_name.field-name.field$name");
        assertArrayEquals(new String[]{"field_name", "field-name", "field$name"}, result);
    }
}
