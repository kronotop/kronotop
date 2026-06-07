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

package com.kronotop.bucket;

import com.kronotop.internal.JSONUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CollationTest {

    @Test
    void shouldDeserializeWithAllFields() {
        // Behavior: A JSON spec with all fields explicitly set deserializes to a Collation with those exact values.
        byte[] json = """
                {"locale": "en", "strength": 2, "case_level": true, "case_first": "upper", "numeric_ordering": true, "alternate": "shifted", "backwards": true, "normalization": true, "max_variable": "space"}
                """.getBytes();
        Collation collation = JSONUtil.readValue(json, Collation.class);

        assertEquals("en", collation.locale());
        assertEquals(2, collation.strength());
        assertTrue(collation.caseLevel());
        assertEquals("upper", collation.caseFirst());
        assertTrue(collation.numericOrdering());
        assertEquals("shifted", collation.alternate());
        assertTrue(collation.backwards());
        assertTrue(collation.normalization());
        assertEquals("space", collation.maxVariable());
    }

    @Test
    void shouldApplyDefaultValues() {
        // Behavior: A JSON spec with only locale uses defaults for all optional fields.
        byte[] json = """
                {"locale": "en"}
                """.getBytes();
        Collation collation = JSONUtil.readValue(json, Collation.class);

        assertEquals("en", collation.locale());
        assertEquals(3, collation.strength());
        assertFalse(collation.caseLevel());
        assertEquals("off", collation.caseFirst());
        assertFalse(collation.numericOrdering());
        assertEquals("non-ignorable", collation.alternate());
        assertFalse(collation.backwards());
        assertFalse(collation.normalization());
        assertEquals("punct", collation.maxVariable());
    }

    @Test
    void shouldRejectMissingLocale() {
        // Behavior: validate() throws when locale is null.
        Collation collation = Collation.create(null, null, null, null, null, null, null, null, null);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, collation::validate);
        assertTrue(ex.getMessage().contains("locale"));
    }

    @Test
    void shouldRejectBlankLocale() {
        // Behavior: validate() throws when locale is blank.
        Collation collation = Collation.create("  ", null, null, null, null, null, null, null, null);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, collation::validate);
        assertTrue(ex.getMessage().contains("locale"));
    }

    @Test
    void shouldRejectStrengthBelowRange() {
        // Behavior: validate() throws when strength is less than 1.
        Collation collation = Collation.create("en", 0, null, null, null, null, null, null, null);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, collation::validate);
        assertTrue(ex.getMessage().contains("strength"));
    }

    @Test
    void shouldRejectStrengthAboveRange() {
        // Behavior: validate() throws when strength is greater than 5.
        Collation collation = Collation.create("en", 6, null, null, null, null, null, null, null);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, collation::validate);
        assertTrue(ex.getMessage().contains("strength"));
    }

    @Test
    void shouldAcceptStrengthBoundaries() {
        // Behavior: validate() accepts strength values 1 and 5 (boundaries).
        Collation c1 = Collation.create("en", 1, null, null, null, null, null, null, null);
        assertDoesNotThrow(c1::validate);

        Collation c5 = Collation.create("en", 5, null, null, null, null, null, null, null);
        assertDoesNotThrow(c5::validate);
    }

    @Test
    void shouldRejectInvalidCaseFirst() {
        // Behavior: validate() throws when caseFirst is not one of "upper", "lower", "off".
        Collation collation = Collation.create("en", null, null, "bad", null, null, null, null, null);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, collation::validate);
        assertTrue(ex.getMessage().contains("case_first"));
    }

    @Test
    void shouldRejectInvalidAlternate() {
        // Behavior: validate() throws when alternate is not one of "non-ignorable", "shifted".
        Collation collation = Collation.create("en", null, null, null, null, "bad", null, null, null);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, collation::validate);
        assertTrue(ex.getMessage().contains("alternate"));
    }

    @Test
    void shouldRejectInvalidMaxVariable() {
        // Behavior: validate() throws when maxVariable is not one of "punct", "space".
        Collation collation = Collation.create("en", null, null, null, null, null, null, null, "bad");
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, collation::validate);
        assertTrue(ex.getMessage().contains("max_variable"));
    }

    @Test
    void shouldRejectUnsupportedLocale() {
        // Behavior: validate() throws when locale is not a recognized ICU collation locale.
        Collation collation = Collation.create("xyzzy", null, null, null, null, null, null, null, null);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, collation::validate);
        assertTrue(ex.getMessage().contains("Invalid collation locale"));
    }

    @Test
    void shouldSerializeAndDeserializeRoundTrip() {
        // Behavior: A Collation serialized to JSON and deserialized back produces an equal object.
        Collation original = Collation.create("tr", 2, true, "lower", true, "shifted", true, true, "space");
        byte[] json = JSONUtil.writeValueAsBytes(original);
        Collation deserialized = JSONUtil.readValue(json, Collation.class);
        assertEquals(original, deserialized);
    }
}
