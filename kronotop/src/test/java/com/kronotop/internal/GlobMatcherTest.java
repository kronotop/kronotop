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
package com.kronotop.internal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobMatcherTest {

    @Test
    void shouldMatchStar() {
        // Behavior: '*' matches any run of characters, including none
        assertTrue(GlobMatcher.matches("bucket.*", "bucket.query", false));
        assertTrue(GlobMatcher.matches("*", "anything", false));
        assertTrue(GlobMatcher.matches("a*", "a", false));
        assertTrue(GlobMatcher.matches("*query", "bucket.query", false));
        assertFalse(GlobMatcher.matches("bucket.*", "zmap.get", false));
    }

    @Test
    void shouldMatchQuestionMark() {
        // Behavior: '?' matches exactly one character
        assertTrue(GlobMatcher.matches("s?t", "set", false));
        assertFalse(GlobMatcher.matches("s?t", "st", false));
        assertFalse(GlobMatcher.matches("s?t", "seat", false));
    }

    @Test
    void shouldMatchCharacterClassAndRange() {
        // Behavior: '[abc]' matches one listed character and '[a-z]' one character in the range
        assertTrue(GlobMatcher.matches("[gs]et", "get", false));
        assertTrue(GlobMatcher.matches("[gs]et", "set", false));
        assertFalse(GlobMatcher.matches("[gs]et", "let", false));
        assertTrue(GlobMatcher.matches("[a-c]", "b", false));
        assertFalse(GlobMatcher.matches("[a-c]", "d", false));
    }

    @Test
    void shouldMatchNegatedClass() {
        // Behavior: '[^a]' matches one character that is not listed
        assertTrue(GlobMatcher.matches("[^g]et", "set", false));
        assertFalse(GlobMatcher.matches("[^g]et", "get", false));
    }

    @Test
    void shouldMatchEscapedChar() {
        // Behavior: a backslash makes the next character literal
        assertTrue(GlobMatcher.matches("a\\*b", "a*b", false));
        assertFalse(GlobMatcher.matches("a\\*b", "aXb", false));
        assertTrue(GlobMatcher.matches("[\\]]", "]", false));
    }

    @Test
    void shouldMatchCaseInsensitively() {
        // Behavior: with noCase the comparison ignores letter case, also inside classes and ranges
        assertTrue(GlobMatcher.matches("BUCKET.*", "bucket.query", true));
        assertFalse(GlobMatcher.matches("BUCKET.*", "bucket.query", false));
        assertTrue(GlobMatcher.matches("[A-C]", "b", true));
        assertTrue(GlobMatcher.matches("[G]et", "get", true));
    }

    @Test
    void shouldRejectNonMatch() {
        // Behavior: a pattern must consume the whole string
        assertFalse(GlobMatcher.matches("get", "getx", false));
        assertFalse(GlobMatcher.matches("getx", "get", false));
        assertFalse(GlobMatcher.matches("*", "", false));
        assertTrue(GlobMatcher.matches("", "", false));
    }
}
