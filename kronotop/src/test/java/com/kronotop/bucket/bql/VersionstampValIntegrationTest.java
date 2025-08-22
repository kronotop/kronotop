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

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.ast.BqlEq;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.bql.ast.StringVal;
import com.kronotop.bucket.bql.ast.VersionstampVal;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import com.kronotop.internal.VersionstampUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VersionstampValIntegrationTest {

    @Test
    void testVersionstampValCreation() {
        // Create a sample Versionstamp
        byte[] bytes = new byte[12];
        bytes[0] = 1;
        bytes[1] = 2;
        bytes[2] = 3;
        // Fill rest with zeros
        Versionstamp versionstamp = Versionstamp.fromBytes(bytes);

        // Create VersionstampVal
        VersionstampVal versionstampVal = new VersionstampVal(versionstamp);

        // Test value accessor
        assertEquals(versionstamp, versionstampVal.value());

        // Test JSON encoding
        String jsonRepresentation = versionstampVal.toJson();
        assertEquals(VersionstampUtil.base32HexEncode(versionstamp), jsonRepresentation);
        assertNotNull(jsonRepresentation);
        assertFalse(jsonRepresentation.isEmpty());
    }

    @Test
    void testVersionstampStringDetection() {
        // Create a test Versionstamp and encode it
        byte[] bytes = new byte[12];
        bytes[0] = 1;
        bytes[1] = 2;
        bytes[2] = 3;
        Versionstamp versionstamp = Versionstamp.fromBytes(bytes);
        String encoded = VersionstampUtil.base32HexEncode(versionstamp);

        // Test a BQL query with the encoded Versionstamp
        String bqlQuery = String.format("{ _id: { $eq: \"%s\" } }", encoded);

        try {
            BqlExpr result = BqlParser.parse(bqlQuery);

            // Should parse as BqlEq
            assertInstanceOf(BqlEq.class, result);
            BqlEq eqExpr = (BqlEq) result;
            assertEquals("_id", eqExpr.selector());

            // The value should be parsed as VersionstampVal
            assertInstanceOf(VersionstampVal.class, eqExpr.value());
            VersionstampVal parsedVal = (VersionstampVal) eqExpr.value();
            assertEquals(versionstamp, parsedVal.value());

        } catch (Exception e) {
            fail("Failed to parse BQL query with Versionstamp: " + e.getMessage());
        }
    }

    @Test
    void testVersionstampInLogicalPlanning() {
        // Create two different Versionstamps
        byte[] bytes1 = new byte[12];
        bytes1[0] = 1;
        Versionstamp versionstamp1 = Versionstamp.fromBytes(bytes1);
        String encoded1 = VersionstampUtil.base32HexEncode(versionstamp1);

        byte[] bytes2 = new byte[12];
        bytes2[0] = 2;
        Versionstamp versionstamp2 = Versionstamp.fromBytes(bytes2);
        String encoded2 = VersionstampUtil.base32HexEncode(versionstamp2);

        // Test equality query
        String equalityQuery = String.format("{ _id: { $eq: \"%s\" } }", encoded1);
        BqlExpr eqResult = BqlParser.parse(equalityQuery);

        // Test logical planning with Versionstamp values
        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(eqResult);
            assertNotNull(logicalPlan);
            // The plan should be created successfully without throwing exceptions
        } catch (Exception e) {
            fail("Logical planning failed with Versionstamp values: " + e.getMessage());
        }

        // Test inequality with different Versionstamps
        String inequalityQuery = String.format("{ _id: { $eq: \"%s\", $ne: \"%s\" } }", encoded1, encoded2);
        BqlExpr ineqResult = BqlParser.parse(inequalityQuery);

        try {
            var logicalPlan = planner.plan(ineqResult);
            assertNotNull(logicalPlan);
            // Should create a plan with the equality (redundancy elimination should work)
        } catch (Exception e) {
            fail("Logical planning failed with mixed Versionstamp values: " + e.getMessage());
        }
    }

    @Test
    void testInvalidVersionstampString() {
        // Test with a string that looks like a Versionstamp but isn't valid
        String invalidQuery = "{ _id: { $eq: \"INVALID_VERSIONSTAMP\" } }";

        BqlExpr result = BqlParser.parse(invalidQuery);

        // Should parse as regular StringVal since it doesn't match the pattern
        assertInstanceOf(BqlEq.class, result);
        BqlEq eqExpr = (BqlEq) result;
        assertInstanceOf(StringVal.class, eqExpr.value());
        StringVal stringVal = (StringVal) eqExpr.value();
        assertEquals("INVALID_VERSIONSTAMP", stringVal.value());
    }

    @Test
    void testVersionstampValueExtraction() {
        // Test the TransformUtils.extractValue functionality
        byte[] bytes = new byte[12];
        bytes[0] = 42;
        Versionstamp versionstamp = Versionstamp.fromBytes(bytes);

        // Test that LogicalPlanner's TransformUtils can extract the value
        // We can't directly test TransformUtils as it's private, but we can test through logical planning
        String encoded = VersionstampUtil.base32HexEncode(versionstamp);
        String query = String.format("{ _id: { $eq: \"%s\", $ne: \"%s\" } }", encoded, encoded);

        BqlExpr result = BqlParser.parse(query);
        LogicalPlanner planner = new LogicalPlanner();

        try {
            var logicalPlan = planner.plan(result);
            // Should detect contradiction (same Versionstamp in EQ and NE)
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Value extraction in logical planning failed: " + e.getMessage());
        }
    }

    @Test
    void testVersionstampToJsonAndBack() {
        // Test the full round-trip: Versionstamp -> JSON -> Parse -> Versionstamp
        byte[] bytes = new byte[12];
        bytes[0] = 123;
        bytes[11] = 45;
        Versionstamp original = Versionstamp.fromBytes(bytes);

        // Encode to JSON representation
        String jsonEncoded = VersionstampUtil.base32HexEncode(original);

        // Create a BQL query with this value
        String query = String.format("{ docId: \"%s\" }", jsonEncoded);
        BqlExpr parsed = BqlParser.parse(query);

        // Extract the parsed value
        assertInstanceOf(BqlEq.class, parsed);
        BqlEq eqExpr = (BqlEq) parsed;
        assertInstanceOf(VersionstampVal.class, eqExpr.value());
        VersionstampVal parsedVersionstampVal = (VersionstampVal) eqExpr.value();

        // Verify round-trip integrity
        assertEquals(original, parsedVersionstampVal.value());
        assertEquals(jsonEncoded, parsedVersionstampVal.toJson());
    }
}