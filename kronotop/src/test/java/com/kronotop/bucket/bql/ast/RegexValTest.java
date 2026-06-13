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

package com.kronotop.bucket.bql.ast;

import com.kronotop.bucket.bql.BqlParseException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RegexValTest {

    @Test
    void shouldThrowOnNullPattern() {
        // Behavior: a null pattern is rejected as a parse error.
        assertThrows(BqlParseException.class, () -> new RegexVal(null, ""));
    }

    @Test
    void shouldNormalizeNullOptionsToEmpty() {
        // Behavior: null options are normalized to an empty string.
        RegexVal regex = new RegexVal("^foo", null);
        assertEquals("", regex.options());
    }

    @Test
    void shouldThrowOnUnsupportedOption() {
        // Behavior: an unsupported option (x) is rejected at construction time.
        assertThrows(BqlParseException.class, () -> new RegexVal("^foo", "x"));
    }

    @Test
    void shouldThrowOnInvalidPattern() {
        // Behavior: a malformed pattern is rejected at construction time.
        assertThrows(BqlParseException.class, () -> new RegexVal("(unclosed", ""));
    }

    @Test
    void shouldBeEqualForSamePatternAndOptions() {
        // Behavior: equality and hashCode are defined by pattern and options.
        RegexVal a = new RegexVal("^foo", "i");
        RegexVal b = new RegexVal("^foo", "i");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void shouldNotBeEqualForDifferentOptions() {
        // Behavior: differing options make two regex values unequal.
        RegexVal a = new RegexVal("^foo", "i");
        RegexVal b = new RegexVal("^foo", "");
        assertNotEquals(a, b);
    }

    @Test
    void shouldRenderToJson() {
        // Behavior: toJson renders the $regex/$options document form.
        RegexVal regex = new RegexVal("^foo", "i");
        assertEquals("{\"$regex\":\"^foo\",\"$options\":\"i\"}", regex.toJson());
    }
}
