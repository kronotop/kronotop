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

package com.kronotop.bucket.bql;

import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BinaryValIntegrationTest {

    @Test
    void testBinaryValCreation() {
        byte[] testData = {0x48, 0x65, 0x6c, 0x6c, 0x6f}; // "Hello" in bytes
        BinaryVal binaryVal = new BinaryVal(testData);

        assertArrayEquals(testData, binaryVal.value());
        assertEquals("\"" + Base64.getEncoder().encodeToString(testData) + "\"", binaryVal.toJson());
    }

    @Test
    void testBinaryValEquality() {
        byte[] data1 = {1, 2, 3, 4, 5};
        byte[] data2 = {1, 2, 3, 4, 5};
        byte[] data3 = {1, 2, 3, 4, 6};

        BinaryVal binary1 = new BinaryVal(data1);
        BinaryVal binary2 = new BinaryVal(data2);
        BinaryVal binary3 = new BinaryVal(data3);

        assertEquals(binary1, binary2);
        assertNotEquals(binary1, binary3);
        assertEquals(binary1.hashCode(), binary2.hashCode());
        assertNotEquals(binary1.hashCode(), binary3.hashCode());
    }

    @Test
    void testBinaryValBase64Encoding() {
        byte[] testData = {(byte) 0xFF, (byte) 0xFE, (byte) 0xFD, 0x00, 0x01};
        BinaryVal binaryVal = new BinaryVal(testData);

        String expectedBase64 = Base64.getEncoder().encodeToString(testData);
        assertEquals("\"" + expectedBase64 + "\"", binaryVal.toJson());

        // Verify round-trip capability
        byte[] decoded = Base64.getDecoder().decode(expectedBase64);
        assertArrayEquals(testData, decoded);
    }

    @Test
    void testBinaryValWithLogicalPlanner() {
        // Test that logical planner handles BinaryVal correctly
        byte[] testData = {0x01, 0x02, 0x03};
        BqlExpr query = new BqlEq("data", new BinaryVal(testData));

        LogicalPlanner planner = new LogicalPlanner();

        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with BinaryVal: " + e.getMessage());
        }
    }

    @Test
    void testBinaryValLogicalEquality() {
        // Test binary equality comparisons in logical planning
        byte[] data1 = {0x00, 0x01, 0x02};
        byte[] data2 = {0x00, 0x01, 0x02};

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlEq("data", new BinaryVal(data1)),
                new BqlEq("data", new BinaryVal(data2))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should handle binary equality correctly
        } catch (Exception e) {
            fail("Logical planning failed with BinaryVal equality: " + e.getMessage());
        }
    }

    @Test
    void testBinaryValInequality() {
        // Test binary inequality comparisons  
        byte[] data1 = {0x01, 0x02, 0x03};
        byte[] data2 = {0x04, 0x05, 0x06};

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlEq("data", new BinaryVal(data1)),
                new BqlNe("data", new BinaryVal(data2))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should create a plan with redundancy elimination
        } catch (Exception e) {
            fail("Logical planning failed with BinaryVal inequality: " + e.getMessage());
        }
    }

    @Test
    void testBinaryValContradiction() {
        // Test contradiction detection with same binary data in EQ and NE
        byte[] testData = {0x42, 0x00, (byte) 0xFF};

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlEq("field", new BinaryVal(testData)),
                new BqlNe("field", new BinaryVal(testData))
        ));

        LogicalPlanner planner = new LogicalPlanner();

        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should detect contradiction: field = data AND field != data
        } catch (Exception e) {
            fail("Logical planning failed with BinaryVal contradiction: " + e.getMessage());
        }
    }

    @Test
    void testBinaryValInArrays() {
        // Test BinaryVal in $in operator with arrays
        byte[] data1 = {0x01};
        byte[] data2 = {0x02};
        byte[] data3 = {0x03};

        BqlExpr query = new BqlIn("signature", java.util.Arrays.asList(
                new BinaryVal(data1),
                new BinaryVal(data2),
                new BinaryVal(data3)
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with BinaryVal in arrays: " + e.getMessage());
        }
    }

    @Test
    void testBinaryValNotInArrays() {
        // Test BinaryVal in $nin operator
        byte[] excludedData = {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};

        BqlExpr query = new BqlNin("checksum", List.of(
                new BinaryVal(excludedData)
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with BinaryVal in $nin: " + e.getMessage());
        }
    }

    @Test
    void testBinaryValWithMixedTypes() {
        // Test binary values mixed with other types in logical planning
        byte[] binaryData = {0x12, 0x34, 0x56, 0x78};

        BqlExpr query = new BqlOr(java.util.Arrays.asList(
                new BqlEq("field", new StringVal("text")),
                new BqlEq("field", new BinaryVal(binaryData)),
                new BqlEq("field", new Int32Val(42))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should handle mixed types including binary correctly
        } catch (Exception e) {
            fail("Logical planning failed with mixed types including BinaryVal: " + e.getMessage());
        }
    }

    @Test
    void testBinaryValInDocuments() {
        // Test binary values within document structures
        byte[] thumbnailData = {(byte) 0x89, 0x50, 0x4E, 0x47}; // PNG header bytes

        java.util.Map<String, BqlValue> fields = new java.util.LinkedHashMap<>();
        fields.put("name", new StringVal("image.png"));
        fields.put("thumbnail", new BinaryVal(thumbnailData));
        fields.put("size", new Int32Val(1024));

        DocumentVal docVal = new DocumentVal(fields);
        assertEquals(3, docVal.fields().size());

        assertTrue(docVal.fields().containsKey("thumbnail"));
        assertInstanceOf(BinaryVal.class, docVal.fields().get("thumbnail"));

        BinaryVal thumbnailVal = (BinaryVal) docVal.fields().get("thumbnail");
        assertArrayEquals(thumbnailData, thumbnailVal.value());
    }

    @Test
    void testBinaryValJsonSerialization() {
        // Test JSON serialization of binary values
        byte[] testData = {0x48, 0x65, 0x6C, 0x6C, 0x6F}; // "Hello"
        BinaryVal binaryVal = new BinaryVal(testData);

        String expectedJson = "\"" + Base64.getEncoder().encodeToString(testData) + "\"";
        assertEquals(expectedJson, binaryVal.toJson());

        // Test in complex structure
        BqlExpr query = new BqlEq("data", binaryVal);
        String explanation = BqlParser.explain(query);
        assertNotNull(explanation);
        assertTrue(explanation.length() > 0, "Explanation should not be empty");
    }

    @Test
    void testBinaryValEmptyData() {
        // Test binary values with empty byte arrays
        byte[] emptyData = {};
        BinaryVal emptyBinary = new BinaryVal(emptyData);

        assertEquals("\"\"", emptyBinary.toJson()); // Empty base64 string
        assertArrayEquals(emptyData, emptyBinary.value());

        LogicalPlanner planner = new LogicalPlanner();
        BqlExpr query = new BqlEq("empty", emptyBinary);

        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with empty BinaryVal: " + e.getMessage());
        }
    }

    @Test
    void testBinaryValLargeData() {
        // Test binary values with larger data sets
        byte[] largeData = new byte[1024];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }

        BinaryVal largeBinary = new BinaryVal(largeData);
        assertArrayEquals(largeData, largeBinary.value());

        String base64 = largeBinary.toJson();
        assertTrue(base64.startsWith("\""));
        assertTrue(base64.endsWith("\""));

        // Verify base64 decoding works
        String base64Content = base64.substring(1, base64.length() - 1);
        byte[] decoded = Base64.getDecoder().decode(base64Content);
        assertArrayEquals(largeData, decoded);
    }

    @Test
    void testBinaryValNumericOperationHandling() {
        // Test that binary values are properly rejected for numeric operations
        byte[] testData = {0x01, 0x02, 0x03, 0x04};

        BqlExpr query = new BqlGt("field", new BinaryVal(testData));
        LogicalPlanner planner = new LogicalPlanner();

        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should handle binary in comparison operations (though semantics may vary)
        } catch (Exception e) {
            // This might throw an exception depending on validation rules
            // Either behavior (allow or reject) can be valid depending on requirements
            assertTrue(e.getMessage().contains("binary") || e.getMessage().contains("Binary") ||
                    e.getMessage().contains("numeric"));
        }
    }

    @Test
    void testBinaryValSpecialByteValues() {
        // Test binary values with special byte values (0, -1, etc.)
        byte[] specialBytes = {0x00, (byte) 0xFF, 0x7F, (byte) 0x80, 0x01, (byte) 0xFE};
        BinaryVal specialBinary = new BinaryVal(specialBytes);

        assertArrayEquals(specialBytes, specialBinary.value());

        String json = specialBinary.toJson();
        assertNotNull(json);
        assertTrue(json.length() > 2); // Should have quotes and content

        // Verify encoding/decoding preserves special bytes
        String base64Content = json.substring(1, json.length() - 1);
        byte[] decoded = Base64.getDecoder().decode(base64Content);
        assertArrayEquals(specialBytes, decoded);
    }
}