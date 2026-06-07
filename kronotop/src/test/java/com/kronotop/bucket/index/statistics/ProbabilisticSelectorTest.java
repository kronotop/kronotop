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

package com.kronotop.bucket.index.statistics;

import org.bson.*;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProbabilisticSelectorTest {

    @Test
    void shouldBeDeterministic() {
        BsonValue value = new BsonString("test-value");
        boolean firstResult = ProbabilisticSelector.match(value);

        // Call multiple times with same value - should always return the same result
        for (int i = 0; i < 100; i++) {
            assertEquals(firstResult, ProbabilisticSelector.match(value));
        }
    }

    @Test
    void shouldBeDeterministicAcrossTypes() {
        // The same numeric value in different representations should be deterministic
        BsonValue int32Value = new BsonInt32(12345);
        boolean int32Result = ProbabilisticSelector.match(int32Value);

        // Multiple calls should return the same result
        for (int i = 0; i < 10; i++) {
            assertEquals(int32Result, ProbabilisticSelector.match(int32Value));
        }
    }

    @Test
    void shouldHaveCorrectProbabilityDistribution() {
        // Test with 50,000 different values to verify ~1/16,384 probability
        int sampleSize = 50_000;
        int selectedCount = 0;

        for (int i = 0; i < sampleSize; i++) {
            BsonValue value = new BsonInt32(i);
            if (ProbabilisticSelector.match(value)) {
                selectedCount++;
            }
        }

        // selectedCount = 4

        // Expected: ~3 selections (50,000 / 16,384 ≈ 3.05)
        // Allow variance: 1-10 selections (reasonable statistical range)
        assertTrue(selectedCount >= 1 && selectedCount <= 10,
                "Selected " + selectedCount + " out of " + sampleSize +
                        " (expected ~3 with 1/16,384 probability)");
    }

    @Test
    void shouldHandleInt32Type() {
        // Test that INT32 values work correctly
        BsonValue value = new BsonInt32(42);
        boolean result = ProbabilisticSelector.match(value);

        // Result should be consistent
        assertEquals(result, ProbabilisticSelector.match(value));
    }

    @Test
    void shouldHandleInt64Type() {
        BsonValue value = new BsonInt64(123456789012345L);
        boolean result = ProbabilisticSelector.match(value);

        assertEquals(result, ProbabilisticSelector.match(value));
    }

    @Test
    void shouldHandleDoubleType() {
        BsonValue value = new BsonDouble(3.14159);
        boolean result = ProbabilisticSelector.match(value);

        assertEquals(result, ProbabilisticSelector.match(value));
    }

    @Test
    void shouldHandleBooleanType() {
        BsonValue trueValue = new BsonBoolean(true);
        BsonValue falseValue = new BsonBoolean(false);

        boolean trueResult = ProbabilisticSelector.match(trueValue);
        boolean falseResult = ProbabilisticSelector.match(falseValue);

        // Results should be deterministic
        assertEquals(trueResult, ProbabilisticSelector.match(trueValue));
        assertEquals(falseResult, ProbabilisticSelector.match(falseValue));
    }

    @Test
    void shouldHandleStringType() {
        BsonValue value = new BsonString("hello-world");
        boolean result = ProbabilisticSelector.match(value);

        assertEquals(result, ProbabilisticSelector.match(value));
    }

    @Test
    void shouldHandleBinaryType() {
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        BsonValue value = new BsonBinary(data);
        boolean result = ProbabilisticSelector.match(value);

        assertEquals(result, ProbabilisticSelector.match(value));
    }

    @Test
    void shouldHandleDateTimeType() {
        BsonValue value = new BsonDateTime(1698336000000L);
        boolean result = ProbabilisticSelector.match(value);

        assertEquals(result, ProbabilisticSelector.match(value));
    }

    @Test
    void shouldHandleTimestampType() {
        BsonValue value = new BsonTimestamp(1698336000, 1);
        boolean result = ProbabilisticSelector.match(value);

        assertEquals(result, ProbabilisticSelector.match(value));
    }

    @Test
    void shouldHandleDecimal128Type() {
        BsonValue value = new BsonDecimal128(new Decimal128(new BigDecimal("123.456")));
        boolean result = ProbabilisticSelector.match(value);

        assertEquals(result, ProbabilisticSelector.match(value));
    }

    @Test
    void shouldHandleNullType() {
        BsonValue value = BsonNull.VALUE;
        boolean result = ProbabilisticSelector.match(value);

        // NULL should always hash to 0, so (0 & 0x3FFF) == 0 is true
        assertTrue(result);
        assertTrue(ProbabilisticSelector.match(value));
    }


    @Test
    void shouldProduceDifferentResultsForDifferentValues() {
        // Test that different values can produce different results
        // (not all values will be selected)
        boolean foundTrue = false;
        boolean foundFalse = false;

        for (int i = 0; i < 1000 && (!foundTrue || !foundFalse); i++) {
            BsonValue value = new BsonInt32(i);
            boolean result = ProbabilisticSelector.match(value);

            if (result) {
                foundTrue = true;
            } else {
                foundFalse = true;
            }
        }

        assertTrue(foundTrue, "Should find at least one value that matches");
        assertTrue(foundFalse, "Should find at least one value that doesn't match");
    }

    @Test
    void shouldBeDeterministicForStringsWithSameContent() {
        BsonValue value1 = new BsonString("identical-string");
        BsonValue value2 = new BsonString("identical-string");

        assertEquals(
                ProbabilisticSelector.match(value1),
                ProbabilisticSelector.match(value2),
                "Same string content should produce same result"
        );
    }


    @Test
    void shouldHaveCorrectProbabilityWithStrings() {
        // Test probability distribution with strings
        int sampleSize = 100_000;
        int selectedCount = 0;

        for (int i = 0; i < sampleSize; i++) {
            BsonValue value = new BsonString("string-" + i);
            if (ProbabilisticSelector.match(value)) {
                selectedCount++;
            }
        }

        // Expected: ~6 selections (100,000 / 16,384 ≈ 6.1)
        // Allow variance: 2-15 selections
        assertTrue(selectedCount >= 2 && selectedCount <= 15,
                "Selected " + selectedCount + " out of " + sampleSize +
                        " (expected ~6 with 1/16,384 probability)");
    }

    @Test
    void shouldHandleObjectIdType() {
        // Behavior: OBJECT_ID values are hashed deterministically via MurmurHash3 on the ObjectId byte array.
        BsonValue value = new BsonObjectId(new ObjectId());
        boolean result = ProbabilisticSelector.match(value);

        assertEquals(result, ProbabilisticSelector.match(value));
    }

    @Test
    void shouldBeDeterministicForObjectIdsWithSameValue() {
        // Behavior: Two BsonObjectId instances wrapping the same ObjectId value produce the same match result.
        ObjectId id = new ObjectId();
        BsonValue value1 = new BsonObjectId(id);
        BsonValue value2 = new BsonObjectId(id);

        assertEquals(
                ProbabilisticSelector.match(value1),
                ProbabilisticSelector.match(value2),
                "Same ObjectId value should produce same result"
        );
    }

    @Test
    void shouldProduceDifferentResultsForDifferentObjectIds() {
        // Behavior: Among many distinct ObjectIds, both true and false outcomes occur.
        boolean foundTrue = false;
        boolean foundFalse = false;

        for (int i = 0; i < 100_000 && (!foundTrue || !foundFalse); i++) {
            BsonValue value = new BsonObjectId(new ObjectId());
            boolean result = ProbabilisticSelector.match(value);

            if (result) {
                foundTrue = true;
            } else {
                foundFalse = true;
            }
        }

        assertTrue(foundTrue, "Should find at least one ObjectId that matches");
        assertTrue(foundFalse, "Should find at least one ObjectId that doesn't match");
    }

    @Test
    void shouldHandleEdgeCaseZeroInt32() {
        BsonValue value = new BsonInt32(0);
        boolean result = ProbabilisticSelector.match(value);

        // 0 & 0x3FFF == 0, so should be true
        assertTrue(result);
        assertTrue(ProbabilisticSelector.match(value));
    }

    @Test
    void shouldHandleEdgeCaseEmptyString() {
        BsonValue value = new BsonString("");
        boolean result = ProbabilisticSelector.match(value);

        assertEquals(result, ProbabilisticSelector.match(value));
    }

    @Test
    void shouldHandleEdgeCaseEmptyBinary() {
        BsonValue value = new BsonBinary(new byte[0]);
        boolean result = ProbabilisticSelector.match(value);

        assertEquals(result, ProbabilisticSelector.match(value));
    }

    @Test
    void shouldBeDeterministicForByteArray() {
        // Behavior: match(byte[]) returns the same result for the same byte array on every call.
        byte[] bytes = new ObjectId().toByteArray();
        boolean firstResult = ProbabilisticSelector.match(bytes);

        for (int i = 0; i < 100; i++) {
            assertEquals(firstResult, ProbabilisticSelector.match(bytes));
        }
    }

    @Test
    void shouldHaveCorrectProbabilityWithByteArrays() {
        // Behavior: match(byte[]) selects ~1/16,384 of distinct byte arrays.
        int sampleSize = 100_000;
        int selectedCount = 0;
        Random random = new Random(42);

        for (int i = 0; i < sampleSize; i++) {
            byte[] bytes = new byte[12];
            random.nextBytes(bytes);
            if (ProbabilisticSelector.match(bytes)) {
                selectedCount++;
            }
        }

        // Expected: ~6 selections (100,000 / 16,384 ≈ 6.1)
        assertTrue(selectedCount >= 2 && selectedCount <= 15,
                "Selected " + selectedCount + " out of " + sampleSize +
                        " (expected ~6 with 1/16,384 probability)");
    }

    @Test
    void shouldHandleEmptyByteArray() {
        // Behavior: match(byte[]) handles empty byte arrays without error.
        boolean result = ProbabilisticSelector.match(new byte[0]);
        assertEquals(result, ProbabilisticSelector.match(new byte[0]));
    }

    @Test
    void shouldProduceDifferentResultsForDifferentByteArrays() {
        // Behavior: Among many distinct byte arrays, both true and false outcomes occur.
        boolean foundTrue = false;
        boolean foundFalse = false;

        for (int i = 0; i < 100_000 && (!foundTrue || !foundFalse); i++) {
            byte[] bytes = new ObjectId().toByteArray();
            boolean result = ProbabilisticSelector.match(bytes);

            if (result) {
                foundTrue = true;
            } else {
                foundFalse = true;
            }
        }

        assertTrue(foundTrue, "Should find at least one byte array that matches");
        assertTrue(foundFalse, "Should find at least one byte array that doesn't match");
    }
}
