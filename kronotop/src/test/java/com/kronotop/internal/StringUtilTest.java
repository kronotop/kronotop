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

    // toUpperCaseAscii(String) tests

    @Test
    void shouldReturnSameReferenceWhenAlreadyUppercase() {
        // Behavior: Zero-copy optimization returns original reference when no conversion needed.
        String input = "HELLO";
        String result = StringUtil.toUpperCaseAscii(input);
        assertSame(input, result);
    }

    @Test
    void shouldConvertLowercaseToUppercase() {
        // Behavior: Converts all lowercase ASCII letters to uppercase.
        assertEquals("HELLO", StringUtil.toUpperCaseAscii("hello"));
    }

    @Test
    void shouldConvertMixedCaseToUppercase() {
        // Behavior: Converts mixed case strings, only changing lowercase letters.
        assertEquals("HELLO", StringUtil.toUpperCaseAscii("HeLLo"));
    }

    @Test
    void shouldReturnSameReferenceForEmptyStringUppercase() {
        // Behavior: Empty string returns same reference (zero-copy).
        String input = "";
        String result = StringUtil.toUpperCaseAscii(input);
        assertSame(input, result);
    }

    @Test
    void shouldPassThroughNumbersAndSymbolsUppercase() {
        // Behavior: Non-letter characters are unchanged.
        String input = "123!@#";
        String result = StringUtil.toUpperCaseAscii(input);
        assertSame(input, result);
    }

    @Test
    void shouldOnlyConvertAsciiLettersToUppercase() {
        // Behavior: Non-ASCII characters are passed through unchanged.
        assertEquals("CAFé", StringUtil.toUpperCaseAscii("café"));
    }

    @Test
    void shouldConvertRespCommandToUppercase() {
        // Behavior: Typical RESP protocol commands are converted correctly.
        assertEquals("BUCKET.INSERT", StringUtil.toUpperCaseAscii("bucket.insert"));
    }

    // toLowerCaseAscii(String) tests

    @Test
    void shouldReturnSameReferenceWhenAlreadyLowercase() {
        // Behavior: Zero-copy optimization returns original reference when no conversion needed.
        String input = "hello";
        String result = StringUtil.toLowerCaseAscii(input);
        assertSame(input, result);
    }

    @Test
    void shouldConvertUppercaseToLowercase() {
        // Behavior: Converts all uppercase ASCII letters to lowercase.
        assertEquals("hello", StringUtil.toLowerCaseAscii("HELLO"));
    }

    @Test
    void shouldConvertMixedCaseToLowercase() {
        // Behavior: Converts mixed case strings, only changing uppercase letters.
        assertEquals("hello", StringUtil.toLowerCaseAscii("HeLLo"));
    }

    @Test
    void shouldReturnSameReferenceForEmptyStringLowercase() {
        // Behavior: Empty string returns same reference (zero-copy).
        String input = "";
        String result = StringUtil.toLowerCaseAscii(input);
        assertSame(input, result);
    }

    @Test
    void shouldPassThroughNumbersAndSymbolsLowercase() {
        // Behavior: Non-letter characters are unchanged.
        String input = "123!@#";
        String result = StringUtil.toLowerCaseAscii(input);
        assertSame(input, result);
    }

    @Test
    void shouldOnlyConvertAsciiLettersToLowercase() {
        // Behavior: Non-ASCII characters are passed through unchanged.
        assertEquals("cafÉ", StringUtil.toLowerCaseAscii("CAFÉ"));
    }

    // toUpperCaseAscii(byte[]) tests

    @Test
    void shouldConvertByteArrayToUppercaseInPlace() {
        // Behavior: Modifies byte array in-place, converting lowercase to uppercase.
        byte[] bytes = "hello".getBytes();
        boolean modified = StringUtil.toUpperCaseAscii(bytes, 0, bytes.length);
        assertTrue(modified);
        assertArrayEquals("HELLO".getBytes(), bytes);
    }

    @Test
    void shouldReturnFalseWhenByteArrayAlreadyUppercase() {
        // Behavior: Returns false when no conversion was needed.
        byte[] bytes = "HELLO".getBytes();
        boolean modified = StringUtil.toUpperCaseAscii(bytes, 0, bytes.length);
        assertFalse(modified);
    }

    @Test
    void shouldConvertPartialByteArrayToUppercase() {
        // Behavior: Only converts bytes within specified offset and length.
        byte[] bytes = "hello world".getBytes();
        StringUtil.toUpperCaseAscii(bytes, 0, 5);
        assertArrayEquals("HELLO world".getBytes(), bytes);
    }

    @Test
    void shouldConvertByteArrayWithOffsetToUppercase() {
        // Behavior: Offset parameter correctly skips initial bytes.
        byte[] bytes = "hello world".getBytes();
        StringUtil.toUpperCaseAscii(bytes, 6, 5);
        assertArrayEquals("hello WORLD".getBytes(), bytes);
    }

    // toLowerCaseAscii(byte[]) tests

    @Test
    void shouldConvertByteArrayToLowercaseInPlace() {
        // Behavior: Modifies byte array in-place, converting uppercase to lowercase.
        byte[] bytes = "HELLO".getBytes();
        boolean modified = StringUtil.toLowerCaseAscii(bytes, 0, bytes.length);
        assertTrue(modified);
        assertArrayEquals("hello".getBytes(), bytes);
    }

    @Test
    void shouldReturnFalseWhenByteArrayAlreadyLowercase() {
        // Behavior: Returns false when no conversion was needed.
        byte[] bytes = "hello".getBytes();
        boolean modified = StringUtil.toLowerCaseAscii(bytes, 0, bytes.length);
        assertFalse(modified);
    }

    @Test
    void shouldConvertPartialByteArrayToLowercase() {
        // Behavior: Only converts bytes within specified offset and length.
        byte[] bytes = "HELLO WORLD".getBytes();
        StringUtil.toLowerCaseAscii(bytes, 0, 5);
        assertArrayEquals("hello WORLD".getBytes(), bytes);
    }

    @Test
    void shouldConvertByteArrayWithOffsetToLowercase() {
        // Behavior: Offset parameter correctly skips initial bytes.
        byte[] bytes = "HELLO WORLD".getBytes();
        StringUtil.toLowerCaseAscii(bytes, 6, 5);
        assertArrayEquals("HELLO world".getBytes(), bytes);
    }
}
