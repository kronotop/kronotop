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

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.planner.Operator;
import org.bson.BsonInt32;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UpdateOptionsTest {

    @Test
    void shouldRejectDollarStar() {
        // Behavior: Malformed positional operator $* should throw IllegalArgumentException at build time.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                UpdateOptions.builder().set("items.$*.price", new BsonInt32(100)).build());
        assertTrue(ex.getMessage().contains("Invalid positional operator '$*'"));
    }

    @Test
    void shouldRejectDollarCurlyBraces() {
        // Behavior: Malformed positional operator ${} should throw IllegalArgumentException at build time.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                UpdateOptions.builder().set("items.${}.price", new BsonInt32(100)).build());
        assertTrue(ex.getMessage().contains("Invalid positional operator '${}'"));
    }

    @Test
    void shouldRejectDollarParentheses() {
        // Behavior: Malformed positional operator $() should throw IllegalArgumentException at build time.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                UpdateOptions.builder().set("items.$().price", new BsonInt32(100)).build());
        assertTrue(ex.getMessage().contains("Invalid positional operator '$()'"));
    }

    @Test
    void shouldRejectDollarWithTrailingChars() {
        // Behavior: Malformed positional operator $abc should throw IllegalArgumentException at build time.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                UpdateOptions.builder().set("items.$abc.price", new BsonInt32(100)).build());
        assertTrue(ex.getMessage().contains("Invalid positional operator '$abc'"));
    }

    @Test
    void shouldRejectDollarBracketWithNumericIdentifier() {
        // Behavior: Filtered positional operator requires a valid identifier, not numeric like $[123].
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                UpdateOptions.builder().set("items.$[123].price", new BsonInt32(100)).build());
        assertTrue(ex.getMessage().contains("Invalid positional operator '$[123]'"));
    }

    @Test
    void shouldRejectDollarBracketWithSpace() {
        // Behavior: Filtered positional operator cannot have spaces in identifier like $[ ].
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                UpdateOptions.builder().set("items.$[ ].price", new BsonInt32(100)).build());
        assertTrue(ex.getMessage().contains("Invalid positional operator '$[ ]'"));
    }

    @Test
    void shouldRejectUnterminatedBracket() {
        // Behavior: Unterminated bracket $[ should throw IllegalArgumentException.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                UpdateOptions.builder().set("items.$[.price", new BsonInt32(100)).build());
        assertTrue(ex.getMessage().contains("Invalid positional operator '$['"));
    }

    @Test
    void shouldAcceptSimplePositionalOperator() {
        // Behavior: Valid simple positional operator $ should be accepted.
        assertDoesNotThrow(() ->
                UpdateOptions.builder().set("items.$.price", new BsonInt32(100)).build());
    }

    @Test
    void shouldAcceptPositionalAllOperator() {
        // Behavior: Valid positional all operator $[] should be accepted.
        assertDoesNotThrow(() ->
                UpdateOptions.builder().set("items.$[].price", new BsonInt32(100)).build());
    }

    @Test
    void shouldAcceptFilteredPositionalOperator() {
        // Behavior: Valid filtered positional operator $[identifier] should be accepted.
        assertDoesNotThrow(() ->
                UpdateOptions.builder()
                        .set("items.$[elem].price", new BsonInt32(100))
                        .arrayFilter(new ArrayFilter("elem.price", Operator.GT, 50))
                        .build());
    }

    @Test
    void shouldAcceptFilteredPositionalWithUnderscore() {
        // Behavior: Valid filtered positional operator with underscore $[my_elem] should be accepted.
        assertDoesNotThrow(() ->
                UpdateOptions.builder()
                        .set("items.$[my_elem].price", new BsonInt32(100))
                        .arrayFilter(new ArrayFilter("my_elem.price", Operator.GT, 50))
                        .build());
    }

    @Test
    void shouldAcceptFilteredPositionalWithNumbers() {
        // Behavior: Valid filtered positional operator with numbers $[elem1] should be accepted.
        assertDoesNotThrow(() ->
                UpdateOptions.builder()
                        .set("items.$[elem1].price", new BsonInt32(100))
                        .arrayFilter(new ArrayFilter("elem1.price", Operator.GT, 50))
                        .build());
    }

    @Test
    void shouldAcceptPathWithoutPositionalOperator() {
        // Behavior: Regular paths without positional operators should be accepted.
        assertDoesNotThrow(() ->
                UpdateOptions.builder().set("items.price", new BsonInt32(100)).build());
    }

    @Test
    void shouldRejectMalformedOperatorInUnset() {
        // Behavior: Malformed positional operator in $unset should also be validated.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                UpdateOptions.builder().unset("items.$*.price").build());
        assertTrue(ex.getMessage().contains("Invalid positional operator '$*'"));
    }
}
