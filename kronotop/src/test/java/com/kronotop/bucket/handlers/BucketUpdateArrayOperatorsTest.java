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

package com.kronotop.bucket.handlers;

import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BsonHelper;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketQueryArgs;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.*;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class BucketUpdateArrayOperatorsTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldUpdateNestedArrayElementWithDotNotation() {
        // Behavior: UPDATE with $set using dot notation (items.0.price) should update the price
        // field of the first element in the items array.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert a document with an items array containing objects with price fields
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-001"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("name", new BsonString("Widget"))
                                .append("price", new BsonInt32(100))
                                .append("quantity", new BsonInt32(2)),
                        new BsonDocument()
                                .append("name", new BsonString("Gadget"))
                                .append("price", new BsonInt32(200))
                                .append("quantity", new BsonInt32(1)),
                        new BsonDocument()
                                .append("name", new BsonString("Gizmo"))
                                .append("price", new BsonInt32(50))
                                .append("quantity", new BsonInt32(5))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);

        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");
        ObjectId targetObjectId = insertedDocs.keySet().iterator().next();

        // Update the price of the first item (items.0.price) from 100 to 150
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"items.0.price\": 150}}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");
        assertTrue(updatedObjectIds.contains(targetObjectId), "Should have updated the target document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        // Verify the items array
        BsonArray items = BsonHelper.getArray(updatedDocument, "items");
        assertNotNull(items, "Items array should exist");
        assertEquals(3, items.size(), "Items array should have 3 elements");

        // Verify first item's price was updated to 150
        BsonDocument firstItem = items.get(0).asDocument();
        assertEquals("Widget", BsonHelper.getString(firstItem, "name"), "First item name should remain unchanged");
        assertEquals(150, BsonHelper.getInteger(firstItem, "price").intValue(), "First item price should be updated to 150");
        assertEquals(2, BsonHelper.getInteger(firstItem, "quantity").intValue(), "First item quantity should remain unchanged");

        // Verify other items remain unchanged
        BsonDocument secondItem = items.get(1).asDocument();
        assertEquals("Gadget", BsonHelper.getString(secondItem, "name"), "Second item name should remain unchanged");
        assertEquals(200, BsonHelper.getInteger(secondItem, "price").intValue(), "Second item price should remain unchanged");
        assertEquals(1, BsonHelper.getInteger(secondItem, "quantity").intValue(), "Second item quantity should remain unchanged");

        BsonDocument thirdItem = items.get(2).asDocument();
        assertEquals("Gizmo", BsonHelper.getString(thirdItem, "name"), "Third item name should remain unchanged");
        assertEquals(50, BsonHelper.getInteger(thirdItem, "price").intValue(), "Third item price should remain unchanged");
        assertEquals(5, BsonHelper.getInteger(thirdItem, "quantity").intValue(), "Third item quantity should remain unchanged");
    }

    @Test
    void shouldUpdateAllArrayElementsWithPositionalAllOperator() {
        // Behavior: UPDATE with $set using the $[] positional operator (items.$[].price) should
        // update the price field of ALL elements in the items array.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert a document with an items array containing objects with price fields
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-002"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("name", new BsonString("Widget"))
                                .append("price", new BsonInt32(100)),
                        new BsonDocument()
                                .append("name", new BsonString("Gadget"))
                                .append("price", new BsonInt32(200)),
                        new BsonDocument()
                                .append("name", new BsonString("Gizmo"))
                                .append("price", new BsonInt32(300))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);

        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");
        ObjectId targetObjectId = insertedDocs.keySet().iterator().next();

        // Update the price of ALL items using the $[] positional operator
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"items.$[].price\": 999}}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");
        assertTrue(updatedObjectIds.contains(targetObjectId), "Should have updated the target document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        // Verify the items array
        BsonArray items = BsonHelper.getArray(updatedDocument, "items");
        assertNotNull(items, "Items array should exist");
        assertEquals(3, items.size(), "Items array should have 3 elements");

        // Verify ALL items have price updated to 999
        BsonDocument firstItem = items.getFirst().asDocument();
        assertEquals("Widget", BsonHelper.getString(firstItem, "name"), "First item name should remain unchanged");
        assertEquals(999, Objects.requireNonNull(BsonHelper.getInteger(firstItem, "price")).intValue(), "First item price should be updated to 999");

        BsonDocument secondItem = items.get(1).asDocument();
        assertEquals("Gadget", BsonHelper.getString(secondItem, "name"), "Second item name should remain unchanged");
        assertEquals(999, Objects.requireNonNull(BsonHelper.getInteger(secondItem, "price")).intValue(), "Second item price should be updated to 999");

        BsonDocument thirdItem = items.get(2).asDocument();
        assertEquals("Gizmo", BsonHelper.getString(thirdItem, "name"), "Third item name should remain unchanged");
        assertEquals(999, Objects.requireNonNull(BsonHelper.getInteger(thirdItem, "price")).intValue(), "Third item price should be updated to 999");
    }

    @Test
    void shouldUpdateAllArrayElementsOnlyInMatchingDocuments() {
        // Behavior: UPDATE with filter { "grades": { $ne: 100 } } and update { $set: { "grades.$[]": 10 } }
        // should only update documents where the grades array does not contain 100. The $[] operator updates
        // all elements in the array, but the filter determines which documents are updated, not which array
        // elements. Documents containing 100 in grades should remain unchanged.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents with varying grades arrays
        // doc1: grades without 100 - should be updated
        BsonDocument doc1 = new BsonDocument()
                .append("name", new BsonString("Student A"))
                .append("grades", new BsonArray(Arrays.asList(
                        new BsonInt32(85),
                        new BsonInt32(90),
                        new BsonInt32(95)
                )));

        // doc2: grades with 100 - should NOT be updated (filter won't match)
        BsonDocument doc2 = new BsonDocument()
                .append("name", new BsonString("Student B"))
                .append("grades", new BsonArray(Arrays.asList(
                        new BsonInt32(100),
                        new BsonInt32(90),
                        new BsonInt32(85)
                )));

        // doc3: grades without 100 - should be updated
        BsonDocument doc3 = new BsonDocument()
                .append("name", new BsonString("Student C"))
                .append("grades", new BsonArray(Arrays.asList(
                        new BsonInt32(70),
                        new BsonInt32(80),
                        new BsonInt32(90)
                )));

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3)
        );
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(3, insertedDocs.size(), "Should have inserted 3 documents");

        // Update: set all grades to 10 for documents where the grades array does not contain 100
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"grades\": {\"$ne\": 100}}", "{\"$set\": {\"grades.$[]\": 10}}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        // Should have updated 2 documents (doc1 and doc3)
        assertEquals(2, updatedObjectIds.size(), "Should have updated 2 documents");

        // Query all documents and verify the results
        Map<String, BsonDocument> documentsByName = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(3, entries.size(), "Should retrieve exactly 3 documents");

            for (BsonDocument doc : entries) {
                String name = BsonHelper.getString(doc, "name");
                documentsByName.put(name, doc);
            }
        }

        // Verify Student A: all grades should be 10
        BsonDocument studentA = documentsByName.get("Student A");
        assertNotNull(studentA, "Student A should exist");
        BsonArray gradesA = BsonHelper.getArray(studentA, "grades");
        assertNotNull(gradesA);
        assertEquals(3, gradesA.size(), "Student A should have 3 grades");
        for (int i = 0; i < gradesA.size(); i++) {
            assertEquals(10, gradesA.get(i).asInt32().getValue(),
                    "Student A grade at index " + i + " should be 10");
        }

        // Verify Student B: grades should remain unchanged (100, 90, 85)
        BsonDocument studentB = documentsByName.get("Student B");
        assertNotNull(studentB, "Student B should exist");
        BsonArray gradesB = BsonHelper.getArray(studentB, "grades");
        assertNotNull(gradesB);
        assertEquals(3, gradesB.size(), "Student B should have 3 grades");
        assertEquals(100, gradesB.get(0).asInt32().getValue(), "Student B first grade should still be 100");
        assertEquals(90, gradesB.get(1).asInt32().getValue(), "Student B second grade should still be 90");
        assertEquals(85, gradesB.get(2).asInt32().getValue(), "Student B third grade should still be 85");

        // Verify Student C: all grades should be 10
        BsonDocument studentC = documentsByName.get("Student C");
        assertNotNull(studentC, "Student C should exist");
        BsonArray gradesC = BsonHelper.getArray(studentC, "grades");
        assertNotNull(gradesC);
        assertEquals(3, gradesC.size(), "Student C should have 3 grades");
        for (int i = 0; i < gradesC.size(); i++) {
            assertEquals(10, gradesC.get(i).asInt32().getValue(),
                    "Student C grade at index " + i + " should be 10");
        }
    }

    @Test
    void shouldUpdateFilteredArrayElementsWithArrayFilters() {
        // Behavior: UPDATE with $set using filtered positional operator (scores.$[high]) and
        // array_filters should only update array elements matching the filter condition.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert a document with scores array [85, 90, 75, 95, 60]
        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("Test Student"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(85),
                        new BsonInt32(90),
                        new BsonInt32(75),
                        new BsonInt32(95),
                        new BsonInt32(60)
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);

        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: {"$set": {"scores.$[high]": 100}, "array_filters": [{"high": {"$gte": 90}}]}
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"scores.$[high]\": 100}, \"array_filters\": [{\"high\": {\"$gte\": 90}}]}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        // Expected: scores = [85, 100, 75, 100, 60] (only 90 and 95 changed to 100)
        BsonArray scores = BsonHelper.getArray(updatedDocument, "scores");
        assertNotNull(scores, "Scores array should exist");
        assertEquals(5, scores.size(), "Scores array should have 5 elements");
        assertEquals(85, scores.get(0).asInt32().getValue(), "First score should remain 85");
        assertEquals(100, scores.get(1).asInt32().getValue(), "Second score (was 90) should be updated to 100");
        assertEquals(75, scores.get(2).asInt32().getValue(), "Third score should remain 75");
        assertEquals(100, scores.get(3).asInt32().getValue(), "Fourth score (was 95) should be updated to 100");
        assertEquals(60, scores.get(4).asInt32().getValue(), "Fifth score should remain 60");
    }

    @Test
    void shouldUpdateNestedFieldsWithFilteredPositionalOperator() {
        // Behavior: UPDATE with $set using filtered positional operator on nested objects
        // (items.$[expensive].discounted) should update only matching array elements.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert a document with items array containing objects with price fields
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-003"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("name", new BsonString("Cheap Item"))
                                .append("price", new BsonInt32(50)),
                        new BsonDocument()
                                .append("name", new BsonString("Expensive Item 1"))
                                .append("price", new BsonInt32(150)),
                        new BsonDocument()
                                .append("name", new BsonString("Medium Item"))
                                .append("price", new BsonInt32(75)),
                        new BsonDocument()
                                .append("name", new BsonString("Expensive Item 2"))
                                .append("price", new BsonInt32(200))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);

        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: {"$set": {"items.$[expensive].discounted": true}, "array_filters": [{"expensive.price": {"$gt": 100}}]}
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"items.$[expensive].discounted\": true}, \"array_filters\": [{\"expensive.price\": {\"$gt\": 100}}]}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        // Verify items array
        BsonArray items = BsonHelper.getArray(updatedDocument, "items");
        assertNotNull(items, "Items array should exist");
        assertEquals(4, items.size(), "Items array should have 4 elements");

        // Cheap Item (price=50) should NOT have discounted field
        BsonDocument cheapItem = items.get(0).asDocument();
        assertFalse(cheapItem.containsKey("discounted"), "Cheap Item should not have discounted field");

        // Expensive Item 1 (price=150) should have discounted=true
        BsonDocument expensiveItem1 = items.get(1).asDocument();
        assertTrue(expensiveItem1.containsKey("discounted"), "Expensive Item 1 should have discounted field");
        assertTrue(expensiveItem1.getBoolean("discounted").getValue(), "Expensive Item 1 discounted should be true");

        // Medium Item (price=75) should NOT have discounted field
        BsonDocument mediumItem = items.get(2).asDocument();
        assertFalse(mediumItem.containsKey("discounted"), "Medium Item should not have discounted field");

        // Expensive Item 2 (price=200) should have discounted=true
        BsonDocument expensiveItem2 = items.get(3).asDocument();
        assertTrue(expensiveItem2.containsKey("discounted"), "Expensive Item 2 should have discounted field");
        assertTrue(expensiveItem2.getBoolean("discounted").getValue(), "Expensive Item 2 discounted should be true");
    }

    @Test
    void shouldNotModifyDocumentWhenNoArrayElementsMatchFilter() {
        // Behavior: UPDATE with array_filters that match no elements should succeed but
        // leave the document unchanged. The no-op document does not appear in object_ids.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert document with scores array [50, 60, 70]
        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("Low Score Student"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(50),
                        new BsonInt32(60),
                        new BsonInt32(70)
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);

        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: {"$set": {"scores.$[high]": 100}, "array_filters": [{"high": {"$gte": 90}}]}
        // No elements are >= 90, so nothing should change
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"scores.$[high]\": 100}, \"array_filters\": [{\"high\": {\"$gte\": 90}}]}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        // The document is left unchanged, so it does not appear in object_ids
        assertTrue(updatedObjectIds.isEmpty(), "No-op document should not appear in object_ids");

        // Query and verify the document is unchanged
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the document");

        // Expected: scores = [50, 60, 70] (unchanged, no elements >= 90)
        BsonArray scores = BsonHelper.getArray(updatedDocument, "scores");
        assertNotNull(scores, "Scores array should exist");
        assertEquals(3, scores.size(), "Scores array should have 3 elements");
        assertEquals(50, scores.get(0).asInt32().getValue(), "First score should remain 50");
        assertEquals(60, scores.get(1).asInt32().getValue(), "Second score should remain 60");
        assertEquals(70, scores.get(2).asInt32().getValue(), "Third score should remain 70");
    }

    @Test
    void shouldUnsetFilteredArrayElements() {
        // Behavior: UPDATE with $unset using filtered positional operator should remove
        // only array elements matching the filter condition.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert document with grades array [85, 45, 90, 55, 95]
        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("Mixed Grades Student"))
                .append("grades", new BsonArray(Arrays.asList(
                        new BsonInt32(85),
                        new BsonInt32(45),
                        new BsonInt32(90),
                        new BsonInt32(55),
                        new BsonInt32(95)
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);

        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: {"$unset": ["grades.$[fail]"], "array_filters": [{"fail": {"$lt": 60}}]}
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$unset\": [\"grades.$[fail]\"], \"array_filters\": [{\"fail\": {\"$lt\": 60}}]}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        // Expected: grades = [85, 90, 95] (45 and 55 removed)
        BsonArray grades = BsonHelper.getArray(updatedDocument, "grades");
        assertNotNull(grades, "Grades array should exist");
        assertEquals(3, grades.size(), "Grades array should have 3 elements after removing failing grades");
        assertEquals(85, grades.get(0).asInt32().getValue(), "First grade should be 85");
        assertEquals(90, grades.get(1).asInt32().getValue(), "Second grade should be 90");
        assertEquals(95, grades.get(2).asInt32().getValue(), "Third grade should be 95");
    }

    @Test
    void shouldUpdateFilteredElementsOnlyInMatchingDocuments() {
        // Behavior: Query filter and arrayFilter work at different levels - query selects
        // documents, array_filters selects array elements within those documents.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert two documents
        // doc1: {status: "active", scores: [80, 90, 70]}
        BsonDocument doc1 = new BsonDocument()
                .append("status", new BsonString("active"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(80),
                        new BsonInt32(90),
                        new BsonInt32(70)
                )));

        // doc2: {status: "inactive", scores: [85, 95, 75]}
        BsonDocument doc2 = new BsonDocument()
                .append("status", new BsonString("inactive"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(85),
                        new BsonInt32(95),
                        new BsonInt32(75)
                )));

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2)
        );
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(2, insertedDocs.size(), "Should have inserted 2 documents");

        // Update with query {"status": "active"} and array_filters [{"high": {"$gte": 85}}]
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"status\": \"active\"}", "{\"$set\": {\"scores.$[high]\": 100}, \"array_filters\": [{\"high\": {\"$gte\": 85}}]}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        // Should have updated only 1 document (the active one)
        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query all documents and verify results
        Map<String, BsonDocument> documentsByStatus = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size(), "Should retrieve exactly 2 documents");

            for (BsonDocument doc : entries) {
                String status = BsonHelper.getString(doc, "status");
                documentsByStatus.put(status, doc);
            }
        }

        // Verify active document: scores = [80, 100, 70] (only 90 changed - matched query, 90 matched filter)
        BsonDocument activeDoc = documentsByStatus.get("active");
        assertNotNull(activeDoc, "Active document should exist");
        BsonArray activeScores = BsonHelper.getArray(activeDoc, "scores");
        assertNotNull(activeScores);
        assertEquals(3, activeScores.size(), "Active doc should have 3 scores");
        assertEquals(80, activeScores.get(0).asInt32().getValue(), "First score should remain 80");
        assertEquals(100, activeScores.get(1).asInt32().getValue(), "Second score (was 90) should be updated to 100");
        assertEquals(70, activeScores.get(2).asInt32().getValue(), "Third score should remain 70");

        // Verify inactive document: scores = [85, 95, 75] (unchanged - doc didn't match query)
        BsonDocument inactiveDoc = documentsByStatus.get("inactive");
        assertNotNull(inactiveDoc, "Inactive document should exist");
        BsonArray inactiveScores = BsonHelper.getArray(inactiveDoc, "scores");
        assertNotNull(inactiveScores);
        assertEquals(3, inactiveScores.size(), "Inactive doc should have 3 scores");
        assertEquals(85, inactiveScores.get(0).asInt32().getValue(), "First score should remain 85");
        assertEquals(95, inactiveScores.get(1).asInt32().getValue(), "Second score should remain 95");
        assertEquals(75, inactiveScores.get(2).asInt32().getValue(), "Third score should remain 75");
    }

    @Test
    void shouldUpdateWithInOperatorViaCommand() {
        // Behavior: BUCKET.UPDATE with $in operator in array_filters should update only elements
        // whose values are in the specified list.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert document with scores array [85, 90, 75, 95, 60]
        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("In Test Student"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(85),
                        new BsonInt32(90),
                        new BsonInt32(75),
                        new BsonInt32(95),
                        new BsonInt32(60)
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: {"$set": {"scores.$[match]": 100}, "array_filters": [{"match": {"$in": [85, 90]}}]}
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"scores.$[match]\": 100}, \"array_filters\": [{\"match\": {\"$in\": [85, 90]}}]}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        // Expected: scores = [100, 100, 75, 95, 60] (only 85 and 90 changed to 100)
        BsonArray scores = BsonHelper.getArray(updatedDocument, "scores");
        assertNotNull(scores, "Scores array should exist");
        assertEquals(5, scores.size(), "Scores array should have 5 elements");
        assertEquals(100, scores.get(0).asInt32().getValue(), "First score (was 85) should be updated to 100");
        assertEquals(100, scores.get(1).asInt32().getValue(), "Second score (was 90) should be updated to 100");
        assertEquals(75, scores.get(2).asInt32().getValue(), "Third score should remain 75");
        assertEquals(95, scores.get(3).asInt32().getValue(), "Fourth score should remain 95");
        assertEquals(60, scores.get(4).asInt32().getValue(), "Fifth score should remain 60");
    }

    @Test
    void shouldUpdateWithNinOperatorViaCommand() {
        // Behavior: BUCKET.UPDATE with $nin operator in array_filters should update only elements
        // whose values are NOT in the specified list.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert document with scores array [85, 90, 75, 95, 60]
        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("Nin Test Student"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(85),
                        new BsonInt32(90),
                        new BsonInt32(75),
                        new BsonInt32(95),
                        new BsonInt32(60)
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: {"$set": {"scores.$[nomatch]": 0}, "array_filters": [{"nomatch": {"$nin": [85, 90]}}]}
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"scores.$[nomatch]\": 0}, \"array_filters\": [{\"nomatch\": {\"$nin\": [85, 90]}}]}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        // Expected: scores = [85, 90, 0, 0, 0] (75, 95, 60 changed to 0)
        BsonArray scores = BsonHelper.getArray(updatedDocument, "scores");
        assertNotNull(scores, "Scores array should exist");
        assertEquals(5, scores.size(), "Scores array should have 5 elements");
        assertEquals(85, scores.get(0).asInt32().getValue(), "First score should remain 85");
        assertEquals(90, scores.get(1).asInt32().getValue(), "Second score should remain 90");
        assertEquals(0, scores.get(2).asInt32().getValue(), "Third score (was 75) should be updated to 0");
        assertEquals(0, scores.get(3).asInt32().getValue(), "Fourth score (was 95) should be updated to 0");
        assertEquals(0, scores.get(4).asInt32().getValue(), "Fifth score (was 60) should be updated to 0");
    }

    @Test
    void shouldUpdateWithSizeOperatorViaCommand() {
        // Behavior: BUCKET.UPDATE with $size operator in array_filters should update only array
        // elements with the specified size.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert document with nested arrays of different sizes
        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("Size Test"))
                .append("data", new BsonArray(Arrays.asList(
                        new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2))),
                        new BsonArray(Arrays.asList(new BsonInt32(3), new BsonInt32(4), new BsonInt32(5))),
                        new BsonArray(List.of(new BsonInt32(6))),
                        new BsonArray(Arrays.asList(new BsonInt32(7), new BsonInt32(8), new BsonInt32(9)))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: replace arrays with size 3 with [0]
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"data.$[elem]\": [0]}, \"array_filters\": [{\"elem\": {\"$size\": 3}}]}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        BsonArray data = BsonHelper.getArray(updatedDocument, "data");
        assertNotNull(data, "Data array should exist");
        assertEquals(4, data.size(), "Data array should have 4 elements");

        // First: size 2 -> not matched
        assertEquals(2, data.get(0).asArray().size());

        // Second: size 3 -> matched, becomes [0]
        assertEquals(1, data.get(1).asArray().size());
        assertEquals(0, data.get(1).asArray().get(0).asInt32().getValue());

        // Third: size 1 -> not matched
        assertEquals(1, data.get(2).asArray().size());
        assertEquals(6, data.get(2).asArray().get(0).asInt32().getValue());

        // Fourth: size 3 -> matched, becomes [0]
        assertEquals(1, data.get(3).asArray().size());
        assertEquals(0, data.get(3).asArray().get(0).asInt32().getValue());
    }

    @Test
    void shouldUpdateWithExistsOperatorViaCommand() {
        // Behavior: BUCKET.UPDATE with $exists operator in array_filters should update only
        // elements where the specified field exists (or doesn't exist).

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert document with items, some having optional fields
        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("Exists Test"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument().append("name", new BsonString("A")),
                        new BsonDocument().append("name", new BsonString("B")).append("discount", new BsonInt32(10)),
                        new BsonDocument().append("name", new BsonString("C")).append("discount", new BsonInt32(20)),
                        new BsonDocument().append("name", new BsonString("D"))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: add hasDiscount=true for items where discount exists
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"items.$[elem].hasDiscount\": true}, \"array_filters\": [{\"elem.discount\": {\"$exists\": true}}]}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        BsonArray items = BsonHelper.getArray(updatedDocument, "items");
        assertNotNull(items, "Items array should exist");
        assertEquals(4, items.size(), "Items array should have 4 elements");

        // First: no discount -> not matched
        BsonDocument item0 = items.get(0).asDocument();
        assertFalse(item0.containsKey("hasDiscount"), "Item A should not have hasDiscount");

        // Second: has discount -> matched
        BsonDocument item1 = items.get(1).asDocument();
        assertTrue(item1.containsKey("hasDiscount"), "Item B should have hasDiscount");
        assertTrue(item1.getBoolean("hasDiscount").getValue());

        // Third: has discount -> matched
        BsonDocument item2 = items.get(2).asDocument();
        assertTrue(item2.containsKey("hasDiscount"), "Item C should have hasDiscount");
        assertTrue(item2.getBoolean("hasDiscount").getValue());

        // Fourth: no discount -> not matched
        BsonDocument item3 = items.get(3).asDocument();
        assertFalse(item3.containsKey("hasDiscount"), "Item D should not have hasDiscount");
    }

    @Test
    void shouldUpdateWithArrayFiltersAndAdvance() {
        // Behavior: UPDATE with array_filters and LIMIT should update documents in batches,
        // and BUCKET.ADVANCE UPDATE should continue processing remaining documents while
        // maintaining the same array_filters logic across all batches. Documents whose arrays
        // have no matching elements are left unchanged and excluded from object_ids.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert 5 documents with scores arrays containing mixed values
        BsonDocument studentA = new BsonDocument()
                .append("name", new BsonString("Student A"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(85), new BsonInt32(70), new BsonInt32(90))));

        BsonDocument studentB = new BsonDocument()
                .append("name", new BsonString("Student B"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(75), new BsonInt32(82), new BsonInt32(65))));

        BsonDocument studentC = new BsonDocument()
                .append("name", new BsonString("Student C"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(95), new BsonInt32(88), new BsonInt32(92))));

        BsonDocument studentD = new BsonDocument()
                .append("name", new BsonString("Student D"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(60), new BsonInt32(55), new BsonInt32(70))));

        BsonDocument studentE = new BsonDocument()
                .append("name", new BsonString("Student E"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(80), new BsonInt32(85), new BsonInt32(78))));

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.toBytes(studentA),
                BSONUtil.toBytes(studentB),
                BSONUtil.toBytes(studentC),
                BSONUtil.toBytes(studentD),
                BSONUtil.toBytes(studentE)
        );

        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(5, insertedDocs.size(), "Should have inserted 5 documents");

        // Initial update with LIMIT=2 and array_filters
        List<ObjectId> allUpdatedObjectIds = new ArrayList<>();
        int cursorId;

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}",
                    "{\"$set\": {\"scores.$[high]\": 100}, \"array_filters\": [{\"high\": {\"$gte\": 80}}]}",
                    BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);

            allUpdatedObjectIds.addAll(extractObjectIds(msg));
            cursorId = extractCursorId(msg);
        }

        // Initial batch should update 2 documents (LIMIT=2)
        assertEquals(2, allUpdatedObjectIds.size(), "Should have updated 2 documents in first batch");

        // Use BUCKET.ADVANCE UPDATE to continue updating remaining documents
        int maxAdvanceCalls = 10;
        int advanceCalls = 0;

        while (advanceCalls < maxAdvanceCalls) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceUpdate(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);

            List<ObjectId> objectIds = extractObjectIds(msg);

            // If no more entries, we're done
            if (objectIds.isEmpty()) {
                break;
            }

            allUpdatedObjectIds.addAll(objectIds);

            advanceCalls++;
        }

        // Should have updated 4 documents; Student D ([60, 55, 70], no element >= 80) is a no-op
        // and does not appear in object_ids
        assertEquals(4, allUpdatedObjectIds.size(), "Should have updated 4 documents across batches");

        // Query all documents and verify array_filters were applied correctly
        Map<String, BsonDocument> documentsByName = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(5, entries.size(), "Should retrieve all 5 documents");

            for (BsonDocument doc : entries) {
                String name = BsonHelper.getString(doc, "name");
                documentsByName.put(name, doc);
            }
        }

        // Verify Student A: [85, 70, 90] -> [100, 70, 100]
        BsonDocument docA = documentsByName.get("Student A");
        assertNotNull(docA, "Student A should exist");
        BsonArray scoresA = BsonHelper.getArray(docA, "scores");
        assertEquals(100, scoresA.get(0).asInt32().getValue(), "Student A score[0]=85 should become 100");
        assertEquals(70, scoresA.get(1).asInt32().getValue(), "Student A score[1]=70 should remain unchanged");
        assertEquals(100, scoresA.get(2).asInt32().getValue(), "Student A score[2]=90 should become 100");

        // Verify Student B: [75, 82, 65] -> [75, 100, 65]
        BsonDocument docB = documentsByName.get("Student B");
        assertNotNull(docB, "Student B should exist");
        BsonArray scoresB = BsonHelper.getArray(docB, "scores");
        assertEquals(75, scoresB.get(0).asInt32().getValue(), "Student B score[0]=75 should remain unchanged");
        assertEquals(100, scoresB.get(1).asInt32().getValue(), "Student B score[1]=82 should become 100");
        assertEquals(65, scoresB.get(2).asInt32().getValue(), "Student B score[2]=65 should remain unchanged");

        // Verify Student C: [95, 88, 92] -> [100, 100, 100]
        BsonDocument docC = documentsByName.get("Student C");
        assertNotNull(docC, "Student C should exist");
        BsonArray scoresC = BsonHelper.getArray(docC, "scores");
        assertEquals(100, scoresC.get(0).asInt32().getValue(), "Student C score[0]=95 should become 100");
        assertEquals(100, scoresC.get(1).asInt32().getValue(), "Student C score[1]=88 should become 100");
        assertEquals(100, scoresC.get(2).asInt32().getValue(), "Student C score[2]=92 should become 100");

        // Verify Student D: [60, 55, 70] -> [60, 55, 70] (no changes, all < 80)
        BsonDocument docD = documentsByName.get("Student D");
        assertNotNull(docD, "Student D should exist");
        BsonArray scoresD = BsonHelper.getArray(docD, "scores");
        assertEquals(60, scoresD.get(0).asInt32().getValue(), "Student D score[0]=60 should remain unchanged");
        assertEquals(55, scoresD.get(1).asInt32().getValue(), "Student D score[1]=55 should remain unchanged");
        assertEquals(70, scoresD.get(2).asInt32().getValue(), "Student D score[2]=70 should remain unchanged");

        // Verify Student E: [80, 85, 78] -> [100, 100, 78]
        BsonDocument docE = documentsByName.get("Student E");
        assertNotNull(docE, "Student E should exist");
        BsonArray scoresE = BsonHelper.getArray(docE, "scores");
        assertEquals(100, scoresE.get(0).asInt32().getValue(), "Student E score[0]=80 should become 100");
        assertEquals(100, scoresE.get(1).asInt32().getValue(), "Student E score[1]=85 should become 100");
        assertEquals(78, scoresE.get(2).asInt32().getValue(), "Student E score[2]=78 should remain unchanged");
    }

    @Test
    void shouldUpdateWithExplicitEqOperatorViaCommand() {
        // Behavior: BUCKET.UPDATE with explicit $eq operator in array_filters should update
        // only elements whose values exactly equal the specified value.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert document with scores array [85, 90, 75, 85, 60]
        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("Explicit Eq Test Student"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(85),
                        new BsonInt32(90),
                        new BsonInt32(75),
                        new BsonInt32(85),
                        new BsonInt32(60)
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: {"$set": {"scores.$[match]": 100}, "array_filters": [{"match": {"$eq": 85}}]}
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"scores.$[match]\": 100}, \"array_filters\": [{\"match\": {\"$eq\": 85}}]}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        // Expected: scores = [100, 90, 75, 100, 60] (both 85s changed to 100)
        BsonArray scores = BsonHelper.getArray(updatedDocument, "scores");
        assertNotNull(scores, "Scores array should exist");
        assertEquals(5, scores.size(), "Scores array should have 5 elements");
        assertEquals(100, scores.get(0).asInt32().getValue(), "First score (was 85) should be updated to 100");
        assertEquals(90, scores.get(1).asInt32().getValue(), "Second score should remain 90");
        assertEquals(75, scores.get(2).asInt32().getValue(), "Third score should remain 75");
        assertEquals(100, scores.get(3).asInt32().getValue(), "Fourth score (was 85) should be updated to 100");
        assertEquals(60, scores.get(4).asInt32().getValue(), "Fifth score should remain 60");
    }

    @Test
    void shouldUpdateWithNeOperatorInArrayFilter() {
        // Behavior: BUCKET.UPDATE with $ne operator in array_filters should update
        // only elements whose values are NOT equal to the specified value.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert document with scores array [100, 85, 100, 90, 100]
        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("Ne Test Student"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(100),
                        new BsonInt32(85),
                        new BsonInt32(100),
                        new BsonInt32(90),
                        new BsonInt32(100)
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: {"$set": {"scores.$[notHigh]": 50}, "array_filters": [{"notHigh": {"$ne": 100}}]}
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"scores.$[notHigh]\": 50}, \"array_filters\": [{\"notHigh\": {\"$ne\": 100}}]}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        // Expected: scores = [100, 50, 100, 50, 100] (85 and 90 changed to 50)
        BsonArray scores = BsonHelper.getArray(updatedDocument, "scores");
        assertNotNull(scores, "Scores array should exist");
        assertEquals(5, scores.size(), "Scores array should have 5 elements");
        assertEquals(100, scores.get(0).asInt32().getValue(), "First score should remain 100");
        assertEquals(50, scores.get(1).asInt32().getValue(), "Second score (was 85) should be updated to 50");
        assertEquals(100, scores.get(2).asInt32().getValue(), "Third score should remain 100");
        assertEquals(50, scores.get(3).asInt32().getValue(), "Fourth score (was 90) should be updated to 50");
        assertEquals(100, scores.get(4).asInt32().getValue(), "Fifth score should remain 100");
    }

    @Test
    void shouldUpdateWithLteOperatorViaCommand() {
        // Behavior: BUCKET.UPDATE with $lte operator in array_filters should update
        // only elements whose values are less than or equal to the specified value.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert document with scores array [55, 60, 65, 70, 75]
        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("Lte Test Student"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(55),
                        new BsonInt32(60),
                        new BsonInt32(65),
                        new BsonInt32(70),
                        new BsonInt32(75)
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: {"$set": {"scores.$[low]": 0}, "array_filters": [{"low": {"$lte": 60}}]}
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"scores.$[low]\": 0}, \"array_filters\": [{\"low\": {\"$lte\": 60}}]}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        // Expected: scores = [0, 0, 65, 70, 75] (55 and 60 changed to 0)
        BsonArray scores = BsonHelper.getArray(updatedDocument, "scores");
        assertNotNull(scores, "Scores array should exist");
        assertEquals(5, scores.size(), "Scores array should have 5 elements");
        assertEquals(0, scores.get(0).asInt32().getValue(), "First score (was 55) should be updated to 0");
        assertEquals(0, scores.get(1).asInt32().getValue(), "Second score (was 60) should be updated to 0");
        assertEquals(65, scores.get(2).asInt32().getValue(), "Third score should remain 65");
        assertEquals(70, scores.get(3).asInt32().getValue(), "Fourth score should remain 70");
        assertEquals(75, scores.get(4).asInt32().getValue(), "Fifth score should remain 75");
    }

    @Test
    void shouldUpdateWithAllOperatorViaCommand() {
        // Behavior: BUCKET.UPDATE with $all operator in array_filters should update
        // only array elements that are themselves arrays AND contain ALL specified values.
        // Scalars will never match $all.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert document with nested arrays of different contents
        // data = [[1,2], [1,2,3], [1,2,3,4], [3,4,5]]
        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("All Test"))
                .append("data", new BsonArray(Arrays.asList(
                        new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2))),
                        new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3))),
                        new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3), new BsonInt32(4))),
                        new BsonArray(Arrays.asList(new BsonInt32(3), new BsonInt32(4), new BsonInt32(5)))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: replace arrays containing all of [1, 2, 3] with [0]
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"data.$[elem]\": [0]}, \"array_filters\": [{\"elem\": {\"$all\": [1, 2, 3]}}]}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        BsonArray data = BsonHelper.getArray(updatedDocument, "data");
        assertNotNull(data, "Data array should exist");
        assertEquals(4, data.size(), "Data array should have 4 elements");

        // First: [1,2] does NOT match $all: [1,2,3] (missing 3)
        BsonArray first = data.get(0).asArray();
        assertEquals(2, first.size(), "First array should remain unchanged");
        assertEquals(1, first.get(0).asInt32().getValue());
        assertEquals(2, first.get(1).asInt32().getValue());

        // Second: [1,2,3] MATCHES $all: [1,2,3], becomes [0]
        BsonArray second = data.get(1).asArray();
        assertEquals(1, second.size(), "Second array should be replaced with [0]");
        assertEquals(0, second.get(0).asInt32().getValue());

        // Third: [1,2,3,4] MATCHES $all: [1,2,3] (has all required + extra), becomes [0]
        BsonArray third = data.get(2).asArray();
        assertEquals(1, third.size(), "Third array should be replaced with [0]");
        assertEquals(0, third.get(0).asInt32().getValue());

        // Fourth: [3,4,5] does NOT match $all: [1,2,3] (missing 1, 2)
        BsonArray fourth = data.get(3).asArray();
        assertEquals(3, fourth.size(), "Fourth array should remain unchanged");
        assertEquals(3, fourth.get(0).asInt32().getValue());
        assertEquals(4, fourth.get(1).asInt32().getValue());
        assertEquals(5, fourth.get(2).asInt32().getValue());
    }

    @Test
    void shouldNotMatchScalarElementsWithAllOperator() {
        // Behavior: $all should return false for scalar elements (not arrays).
        // This ensures that $all only applies to nested arrays.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert document with mixed data: scalars and a nested array
        // data = [1, 2, 3, [1, 2, 3]]
        BsonDocument document = new BsonDocument()
                .append("name", new BsonString("All Scalar Test"))
                .append("data", new BsonArray(Arrays.asList(
                        new BsonInt32(1),
                        new BsonInt32(2),
                        new BsonInt32(3),
                        new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: replace arrays containing all of [1] with [0]
        // Only the nested array [1,2,3] should match, scalars should not
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"data.$[elem]\": [0]}, \"array_filters\": [{\"elem\": {\"$all\": [1]}}]}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        BsonArray data = BsonHelper.getArray(updatedDocument, "data");
        assertNotNull(data, "Data array should exist");
        assertEquals(4, data.size(), "Data array should have 4 elements");

        // Scalars should remain unchanged
        assertEquals(1, data.get(0).asInt32().getValue(), "First element (scalar 1) should remain unchanged");
        assertEquals(2, data.get(1).asInt32().getValue(), "Second element (scalar 2) should remain unchanged");
        assertEquals(3, data.get(2).asInt32().getValue(), "Third element (scalar 3) should remain unchanged");

        // Only the nested array [1,2,3] should be replaced with [0]
        BsonArray fourth = data.get(3).asArray();
        assertEquals(1, fourth.size(), "Fourth element (nested array) should be replaced with [0]");
        assertEquals(0, fourth.get(0).asInt32().getValue());
    }

    @Test
    void shouldUpdateArrayElementsWhenFilterUsesElemMatch() {
        // Behavior: When query filter uses $elemMatch to match documents, and update uses $[identifier]
        // with array_filters, only matching documents should be updated and only matching array elements
        // should be modified.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Doc1: items with statuses ["pending", "done", "pending"]
        BsonDocument doc1 = new BsonDocument()
                .append("name", new BsonString("Order1"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("name", new BsonString("Item A"))
                                .append("status", new BsonString("pending")),
                        new BsonDocument()
                                .append("name", new BsonString("Item B"))
                                .append("status", new BsonString("done")),
                        new BsonDocument()
                                .append("name", new BsonString("Item C"))
                                .append("status", new BsonString("pending"))
                )));

        // Doc2: items with statuses ["done", "done"]
        BsonDocument doc2 = new BsonDocument()
                .append("name", new BsonString("Order2"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("name", new BsonString("Item D"))
                                .append("status", new BsonString("done")),
                        new BsonDocument()
                                .append("name", new BsonString("Item E"))
                                .append("status", new BsonString("done"))
                )));

        // Doc3: items with statuses ["pending"]
        BsonDocument doc3 = new BsonDocument()
                .append("name", new BsonString("Order3"))
                .append("items", new BsonArray(Collections.singletonList(
                        new BsonDocument()
                                .append("name", new BsonString("Item F"))
                                .append("status", new BsonString("pending"))
                )));

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3)
        );
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(3, insertedDocs.size(), "Should have inserted 3 documents");

        // Update: Filter with $elemMatch, update with array_filters
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{\"items\": {\"$elemMatch\": {\"status\": {\"$eq\": \"pending\"}}}}",
                    "{\"$set\": {\"items.$[elem].status\": \"done\"}, \"array_filters\": [{\"elem.status\": {\"$eq\": \"pending\"}}]}"
            ).encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        // Should have updated 2 documents (Doc1 and Doc3, not Doc2)
        assertEquals(2, updatedObjectIds.size(), "Should have updated 2 documents");

        // Query all documents and verify results
        Map<String, BsonDocument> documentsByName = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(3, entries.size(), "Should retrieve exactly 3 documents");

            for (BsonDocument doc : entries) {
                String name = BsonHelper.getString(doc, "name");
                documentsByName.put(name, doc);
            }
        }

        // Verify Order1: all "pending" statuses should be changed to "done"
        BsonDocument order1 = documentsByName.get("Order1");
        assertNotNull(order1, "Order1 should exist");
        BsonArray items1 = BsonHelper.getArray(order1, "items");
        assertEquals(3, items1.size());
        // Assert that only matching array elements were actually modified
        assertFalse(items1.get(1).asDocument().containsKey("updatedAt"),
                "Array elements that do not match array_filters must not receive update side-effects");
        assertEquals("done", BsonHelper.getString(items1.get(0).asDocument(), "status"), "Item A should be done");
        assertEquals("done", BsonHelper.getString(items1.get(1).asDocument(), "status"), "Item B should be done");
        assertEquals("done", BsonHelper.getString(items1.get(2).asDocument(), "status"), "Item C should be done");

        // Verify Order2: unchanged (no pending items, so the filter didn't match)
        BsonDocument order2 = documentsByName.get("Order2");
        assertNotNull(order2, "Order2 should exist");
        BsonArray items2 = BsonHelper.getArray(order2, "items");
        assertEquals(2, items2.size());
        assertEquals("done", BsonHelper.getString(items2.get(0).asDocument(), "status"), "Item D should still be done");
        assertEquals("done", BsonHelper.getString(items2.get(1).asDocument(), "status"), "Item E should still be done");

        // Verify Order3: "pending" status should be changed to "done"
        BsonDocument order3 = documentsByName.get("Order3");
        assertNotNull(order3, "Order3 should exist");
        BsonArray items3 = BsonHelper.getArray(order3, "items");
        assertEquals(1, items3.size());
        assertEquals("done", BsonHelper.getString(items3.get(0).asDocument(), "status"), "Item F should be done");
    }

    @Test
    void shouldUpdateArrayElementsWithElemMatchOnNestedFields() {
        // Behavior: $elemMatch with nested field conditions combined with array_filters targeting
        // the same nested structure.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Document with orders array containing objects with nested item.price field
        BsonDocument document = new BsonDocument()
                .append("customerId", new BsonString("CUST-001"))
                .append("orders", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("orderId", new BsonString("ORD-A"))
                                .append("item", new BsonDocument()
                                        .append("name", new BsonString("Widget"))
                                        .append("price", new BsonInt32(50))),
                        new BsonDocument()
                                .append("orderId", new BsonString("ORD-B"))
                                .append("item", new BsonDocument()
                                        .append("name", new BsonString("Gadget"))
                                        .append("price", new BsonInt32(150))),
                        new BsonDocument()
                                .append("orderId", new BsonString("ORD-C"))
                                .append("item", new BsonDocument()
                                        .append("name", new BsonString("Premium Widget"))
                                        .append("price", new BsonInt32(250)))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: Filter with $elemMatch on the nested field, update with array_filters
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{\"orders\": {\"$elemMatch\": {\"item.price\": {\"$gt\": 100}}}}",
                    "{\"$set\": {\"orders.$[expensive].discounted\": true}, \"array_filters\": [{\"expensive.item.price\": {\"$gt\": 100}}]}"
            ).encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        BsonArray orders = BsonHelper.getArray(updatedDocument, "orders");
        assertNotNull(orders, "Orders array should exist");
        assertEquals(3, orders.size(), "Orders array should have 3 elements");

        // Order A (price=50): should NOT have a discounted field
        BsonDocument orderA = orders.get(0).asDocument();
        assertEquals("ORD-A", BsonHelper.getString(orderA, "orderId"));
        assertFalse(orderA.containsKey("discounted"), "Order A should not have discounted field");

        // Order B (price=150): should have discounted=true
        BsonDocument orderB = orders.get(1).asDocument();
        assertEquals("ORD-B", BsonHelper.getString(orderB, "orderId"));
        assertTrue(orderB.containsKey("discounted"), "Order B should have discounted field");
        assertTrue(orderB.getBoolean("discounted").getValue(), "Order B discounted should be true");

        // Order C (price=250): should have discounted=true
        BsonDocument orderC = orders.get(2).asDocument();
        assertEquals("ORD-C", BsonHelper.getString(orderC, "orderId"));
        assertTrue(orderC.containsKey("discounted"), "Order C should have discounted field");
        assertTrue(orderC.getBoolean("discounted").getValue(), "Order C discounted should be true");

        // Optional: ensure correct array filter identifier resolution
        assertTrue(orderB.containsKey("discounted") && orderC.containsKey("discounted"),
                "Nested array filters must correctly resolve and apply using the base identifier");
    }

    @Test
    void shouldNotUpdateNonMatchingDocumentsWithElemMatchOnNestedFields() {
        // Behavior: When multiple documents exist, $elemMatch filter only updates documents where
        // at least one array element matches. Documents where no array element matches remain unchanged.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Document 1: Has orders with price > 100 (should match $elemMatch)
        BsonDocument document1 = new BsonDocument()
                .append("customerId", new BsonString("CUST-001"))
                .append("orders", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("orderId", new BsonString("ORD-A"))
                                .append("item", new BsonDocument()
                                        .append("name", new BsonString("Widget"))
                                        .append("price", new BsonInt32(50))),
                        new BsonDocument()
                                .append("orderId", new BsonString("ORD-B"))
                                .append("item", new BsonDocument()
                                        .append("name", new BsonString("Gadget"))
                                        .append("price", new BsonInt32(150)))
                )));

        // Document 2: ALL orders have price <= 100 (should NOT match $elemMatch)
        BsonDocument document2 = new BsonDocument()
                .append("customerId", new BsonString("CUST-002"))
                .append("orders", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("orderId", new BsonString("ORD-X"))
                                .append("item", new BsonDocument()
                                        .append("name", new BsonString("Basic Item"))
                                        .append("price", new BsonInt32(30))),
                        new BsonDocument()
                                .append("orderId", new BsonString("ORD-Y"))
                                .append("item", new BsonDocument()
                                        .append("name", new BsonString("Simple Tool"))
                                        .append("price", new BsonInt32(75))),
                        new BsonDocument()
                                .append("orderId", new BsonString("ORD-Z"))
                                .append("item", new BsonDocument()
                                        .append("name", new BsonString("Standard Part"))
                                        .append("price", new BsonInt32(100)))
                )));

        List<byte[]> testDocuments = Arrays.asList(BSONUtil.toBytes(document1), BSONUtil.toBytes(document2));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(2, insertedDocs.size(), "Should have inserted 2 documents");

        // Update: Filter with $elemMatch on price > 100, should only match CUST-001
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{\"orders\": {\"$elemMatch\": {\"item.price\": {\"$gt\": 100}}}}",
                    "{\"$set\": {\"orders.$[expensive].discounted\": true}, \"array_filters\": [{\"expensive.item.price\": {\"$gt\": 100}}]}"
            ).encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated only 1 document (CUST-001)");

        // Query all documents and verify
        Map<String, BsonDocument> documents = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size(), "Should retrieve exactly 2 documents");

            for (BsonDocument doc : entries) {
                String customerId = BsonHelper.getString(doc, "customerId");
                documents.put(customerId, doc);
            }
        }

        // Verify CUST-001 was updated
        BsonDocument cust001 = documents.get("CUST-001");
        assertNotNull(cust001, "CUST-001 should exist");
        BsonArray orders001 = BsonHelper.getArray(cust001, "orders");
        assertEquals(2, orders001.size(), "CUST-001 should have 2 orders");

        BsonDocument ordA = orders001.get(0).asDocument();
        assertEquals("ORD-A", BsonHelper.getString(ordA, "orderId"));
        assertFalse(ordA.containsKey("discounted"), "ORD-A (price=50) should NOT have discounted field");

        BsonDocument ordB = orders001.get(1).asDocument();
        assertEquals("ORD-B", BsonHelper.getString(ordB, "orderId"));
        assertTrue(ordB.containsKey("discounted"), "ORD-B (price=150) should have discounted field");
        assertTrue(ordB.getBoolean("discounted").getValue(), "ORD-B discounted should be true");

        // Verify CUST-002 was NOT updated (no discounted field on any order)
        BsonDocument cust002 = documents.get("CUST-002");
        assertNotNull(cust002, "CUST-002 should exist");
        BsonArray orders002 = BsonHelper.getArray(cust002, "orders");
        assertEquals(3, orders002.size(), "CUST-002 should have 3 orders");

        for (int i = 0; i < orders002.size(); i++) {
            BsonDocument order = orders002.get(i).asDocument();
            assertFalse(order.containsKey("discounted"),
                    "CUST-002 order " + BsonHelper.getString(order, "orderId") + " should NOT have discounted field");
        }
    }

    @Test
    void shouldNotUpdateDocumentsWhenElemMatchFindsNoMatch() {
        // Behavior: When $elemMatch filter matches no documents, no updates should occur.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents where no array element matches the $elemMatch condition
        BsonDocument doc1 = new BsonDocument()
                .append("name", new BsonString("Order1"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("name", new BsonString("Item A"))
                                .append("status", new BsonString("done")),
                        new BsonDocument()
                                .append("name", new BsonString("Item B"))
                                .append("status", new BsonString("shipped"))
                )));

        BsonDocument doc2 = new BsonDocument()
                .append("name", new BsonString("Order2"))
                .append("items", new BsonArray(Collections.singletonList(
                        new BsonDocument()
                                .append("name", new BsonString("Item C"))
                                .append("status", new BsonString("completed"))
                )));

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2)
        );
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(2, insertedDocs.size(), "Should have inserted 2 documents");

        // Update with $elemMatch filter that matches nothing (status = "archived")
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{\"items\": {\"$elemMatch\": {\"status\": {\"$eq\": \"archived\"}}}}",
                    "{\"$set\": {\"items.$[elem].status\": \"done\"}, \"array_filters\": [{\"elem.status\": {\"$eq\": \"archived\"}}]}"
            ).encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        // Should have updated 0 documents (no documents match the filter)
        assertEquals(0, updatedObjectIds.size(), "Should have updated 0 documents");

        // Verify all documents remain unchanged
        Map<String, BsonDocument> documentsByName = new LinkedHashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size(), "Should retrieve exactly 2 documents");

            for (BsonDocument doc : entries) {
                String name = BsonHelper.getString(doc, "name");
                documentsByName.put(name, doc);
            }
        }

        // Verify Order1 unchanged
        BsonDocument order1 = documentsByName.get("Order1");
        BsonArray items1 = BsonHelper.getArray(order1, "items");
        assertEquals("done", BsonHelper.getString(items1.get(0).asDocument(), "status"));
        assertEquals("shipped", BsonHelper.getString(items1.get(1).asDocument(), "status"));

        // Verify Order2 unchanged
        BsonDocument order2 = documentsByName.get("Order2");
        BsonArray items2 = BsonHelper.getArray(order2, "items");
        assertEquals("completed", BsonHelper.getString(items2.get(0).asDocument(), "status"));
    }

    @Test
    void shouldUpdateMultipleFieldsWithElemMatchFilter() {
        // Behavior: $elemMatch filter combined with multiple $set operations on array elements.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Document with tasks array
        BsonDocument document = new BsonDocument()
                .append("projectId", new BsonString("PROJ-001"))
                .append("tasks", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("name", new BsonString("Task A"))
                                .append("priority", new BsonString("low")),
                        new BsonDocument()
                                .append("name", new BsonString("Task B"))
                                .append("priority", new BsonString("high")),
                        new BsonDocument()
                                .append("name", new BsonString("Task C"))
                                .append("priority", new BsonString("high")),
                        new BsonDocument()
                                .append("name", new BsonString("Task D"))
                                .append("priority", new BsonString("medium"))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: Filter with $elemMatch, update multiple fields on matching array elements
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{\"tasks\": {\"$elemMatch\": {\"priority\": {\"$eq\": \"high\"}}}}",
                    "{\"$set\": {\"tasks.$[t].completed\": true, \"tasks.$[t].completedAt\": \"2024-01-01\"}, \"array_filters\": [{\"t.priority\": {\"$eq\": \"high\"}}]}"
            ).encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        BsonArray tasks = BsonHelper.getArray(updatedDocument, "tasks");
        assertNotNull(tasks, "Tasks array should exist");
        assertEquals(4, tasks.size(), "Tasks array should have 4 elements");

        // Task A (priority=low): should NOT have completed or completedAt fields
        BsonDocument taskA = tasks.get(0).asDocument();
        assertEquals("Task A", BsonHelper.getString(taskA, "name"));
        assertFalse(taskA.containsKey("completed"), "Task A should not have completed field");
        assertFalse(taskA.containsKey("completedAt"), "Task A should not have completedAt field");

        // Task B (priority=high): should have both fields
        BsonDocument taskB = tasks.get(1).asDocument();
        assertEquals("Task B", BsonHelper.getString(taskB, "name"));
        assertTrue(taskB.containsKey("completed"), "Task B should have completed field");
        assertTrue(taskB.getBoolean("completed").getValue(), "Task B completed should be true");
        assertEquals("2024-01-01", BsonHelper.getString(taskB, "completedAt"), "Task B completedAt should be set");

        // Task C (priority=high): should have both fields
        BsonDocument taskC = tasks.get(2).asDocument();
        assertEquals("Task C", BsonHelper.getString(taskC, "name"));
        assertTrue(taskC.containsKey("completed"), "Task C should have completed field");
        assertTrue(taskC.getBoolean("completed").getValue(), "Task C completed should be true");
        assertEquals("2024-01-01", BsonHelper.getString(taskC, "completedAt"), "Task C completedAt should be set");

        // Task D (priority=medium): should NOT have completed or completedAt fields
        BsonDocument taskD = tasks.get(3).asDocument();
        assertEquals("Task D", BsonHelper.getString(taskD, "name"));
        assertFalse(taskD.containsKey("completed"), "Task D should not have completed field");
        assertFalse(taskD.containsKey("completedAt"), "Task D should not have completedAt field");
    }

    @Test
    void shouldUpdateArrayElementsWithMultipleArrayFiltersOnNestedFields() {
        // Behavior: Multiple array_filters can target the same array with different conditions,
        // applying different updates to different subsets of array elements based on nested field values.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Document with orders array containing objects with nested item.price field
        BsonDocument document = new BsonDocument()
                .append("customerId", new BsonString("CUST-002"))
                .append("orders", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("orderId", new BsonString("ORD-A"))
                                .append("item", new BsonDocument()
                                        .append("name", new BsonString("Basic Widget"))
                                        .append("price", new BsonInt32(50))),
                        new BsonDocument()
                                .append("orderId", new BsonString("ORD-B"))
                                .append("item", new BsonDocument()
                                        .append("name", new BsonString("Standard Gadget"))
                                        .append("price", new BsonInt32(100))),
                        new BsonDocument()
                                .append("orderId", new BsonString("ORD-C"))
                                .append("item", new BsonDocument()
                                        .append("name", new BsonString("Premium Device"))
                                        .append("price", new BsonInt32(150))),
                        new BsonDocument()
                                .append("orderId", new BsonString("ORD-D"))
                                .append("item", new BsonDocument()
                                        .append("name", new BsonString("Luxury Item"))
                                        .append("price", new BsonInt32(250)))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update with multiple array_filters:
        // - expensive: item.price > 100 -> set discounted=true
        // - cheap: item.price <= 100 -> set note="no discount"
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{}",
                    "{\"$set\": {\"orders.$[expensive].discounted\": true, \"orders.$[cheap].note\": \"no discount\"}, " +
                            "\"array_filters\": [{\"expensive.item.price\": {\"$gt\": 100}}, {\"cheap.item.price\": {\"$lte\": 100}}]}"
            ).encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        BsonArray orders = BsonHelper.getArray(updatedDocument, "orders");
        assertNotNull(orders, "Orders array should exist");
        assertEquals(4, orders.size(), "Orders array should have 4 elements");

        // Order A (price=50): should have note="no discount", no discounted field
        BsonDocument orderA = orders.get(0).asDocument();
        assertEquals("ORD-A", BsonHelper.getString(orderA, "orderId"));
        assertFalse(orderA.containsKey("discounted"), "Order A should NOT have discounted field");
        assertTrue(orderA.containsKey("note"), "Order A should have note field");
        assertEquals("no discount", BsonHelper.getString(orderA, "note"), "Order A note should be 'no discount'");

        // Order B (price=100): should have note="no discount", no discounted field
        BsonDocument orderB = orders.get(1).asDocument();
        assertEquals("ORD-B", BsonHelper.getString(orderB, "orderId"));
        assertFalse(orderB.containsKey("discounted"), "Order B should NOT have discounted field");
        assertTrue(orderB.containsKey("note"), "Order B should have note field");
        assertEquals("no discount", BsonHelper.getString(orderB, "note"), "Order B note should be 'no discount'");

        // Order C (price=150): should have discounted=true, no note field
        BsonDocument orderC = orders.get(2).asDocument();
        assertEquals("ORD-C", BsonHelper.getString(orderC, "orderId"));
        assertTrue(orderC.containsKey("discounted"), "Order C should have discounted field");
        assertTrue(orderC.getBoolean("discounted").getValue(), "Order C discounted should be true");
        assertFalse(orderC.containsKey("note"), "Order C should NOT have note field");

        // Order D (price=250): should have discounted=true, no note field
        BsonDocument orderD = orders.get(3).asDocument();
        assertEquals("ORD-D", BsonHelper.getString(orderD, "orderId"));
        assertTrue(orderD.containsKey("discounted"), "Order D should have discounted field");
        assertTrue(orderD.getBoolean("discounted").getValue(), "Order D discounted should be true");
        assertFalse(orderD.containsKey("note"), "Order D should NOT have note field");
    }

    // ==================== $ Positional Operator Tests ====================

    @Test
    void shouldUpdateFirstMatchingArrayElementWithPositionalOperator() {
        // Behavior: UPDATE with $set using the $ positional operator (items.$.status) should update
        // ONLY the first element in the items array that matches the query condition.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert a document with items array containing objects with status fields
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-POS-001"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("name", new BsonString("Widget"))
                                .append("status", new BsonString("pending")),
                        new BsonDocument()
                                .append("name", new BsonString("Gadget"))
                                .append("status", new BsonString("pending")),
                        new BsonDocument()
                                .append("name", new BsonString("Gizmo"))
                                .append("status", new BsonString("shipped"))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update: query matches items with status "pending", update uses $ to update first match only
        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"items.status\": \"pending\"}", "{\"$set\": {\"items.$.status\": \"processed\"}}").encode(buf);
            Object msg = runCommand(channel, buf);

            updatedObjectIds = extractObjectIds(msg);
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Query and verify the update
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(1, entries.size(), "Should retrieve exactly 1 document");
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument, "Should have retrieved the updated document");

        BsonArray items = BsonHelper.getArray(updatedDocument, "items");
        assertNotNull(items, "Items array should exist");
        assertEquals(3, items.size(), "Items array should have 3 elements");

        // First item (Widget) - SHOULD be updated to "processed" (first match)
        BsonDocument firstItem = items.get(0).asDocument();
        assertEquals("Widget", BsonHelper.getString(firstItem, "name"));
        assertEquals("processed", BsonHelper.getString(firstItem, "status"), "First matching item should be updated to 'processed'");

        // Second item (Gadget) - should STILL be "pending" (not updated, only first match)
        BsonDocument secondItem = items.get(1).asDocument();
        assertEquals("Gadget", BsonHelper.getString(secondItem, "name"));
        assertEquals("pending", BsonHelper.getString(secondItem, "status"), "Second item should remain 'pending'");

        // Third item (Gizmo) - should still be "shipped" (didn't match query)
        BsonDocument thirdItem = items.get(2).asDocument();
        assertEquals("Gizmo", BsonHelper.getString(thirdItem, "name"));
        assertEquals("shipped", BsonHelper.getString(thirdItem, "status"), "Third item should remain 'shipped'");
    }

    @Test
    void shouldUpdateNestedFieldInMatchingArrayElement() {
        // Behavior: UPDATE with $ positional operator can update nested fields within the matched element.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-POS-002"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("name", new BsonString("Widget"))
                                .append("details", new BsonDocument()
                                        .append("processed", new BsonBoolean(false))
                                        .append("priority", new BsonInt32(1))),
                        new BsonDocument()
                                .append("name", new BsonString("Gadget"))
                                .append("details", new BsonDocument()
                                        .append("processed", new BsonBoolean(false))
                                        .append("priority", new BsonInt32(2)))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(1, insertedDocs.size(), "Should have inserted 1 document");

        // Update nested field in first matching element
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"items.details.priority\": 1}", "{\"$set\": {\"items.$.details.processed\": true}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
        }

        // Query and verify
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            updatedDocument = entries.getFirst();
        }

        assertNotNull(updatedDocument);
        BsonArray items = BsonHelper.getArray(updatedDocument, "items");

        // The first item-nested field should be updated
        assertNotNull(items);
        BsonDocument firstItem = items.get(0).asDocument();
        assertTrue(firstItem.getDocument("details").getBoolean("processed").getValue(),
                "First item's nested 'processed' field should be true");

        // Second item - should remain unchanged
        BsonDocument secondItem = items.get(1).asDocument();
        assertFalse(secondItem.getDocument("details").getBoolean("processed").getValue(),
                "Second item's nested 'processed' field should remain false");
    }

    @Test
    void shouldOnlyUpdateFirstMatchWhenMultipleElementsMatch() {
        // Behavior: When multiple array elements match the query, only the first matching element is updated.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-POS-003"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonDocument().append("value", new BsonInt32(85)).append("checked", new BsonBoolean(false)),
                        new BsonDocument().append("value", new BsonInt32(90)).append("checked", new BsonBoolean(false)),
                        new BsonDocument().append("value", new BsonInt32(95)).append("checked", new BsonBoolean(false))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Query matches all elements >= 85 (all three), but $ updates only the first
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"scores.value\": {\"$gte\": 85}}", "{\"$set\": {\"scores.$.checked\": true}}").encode(buf);
            runCommand(channel, buf);
        }

        // Verify
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            updatedDocument = entries.getFirst();
        }

        BsonArray scores = BsonHelper.getArray(updatedDocument, "scores");

        // Only the first element should be updated
        assertNotNull(scores);
        assertTrue(scores.get(0).asDocument().getBoolean("checked").getValue(), "First element should be checked=true");
        assertFalse(scores.get(1).asDocument().getBoolean("checked").getValue(), "Second element should remain checked=false");
        assertFalse(scores.get(2).asDocument().getBoolean("checked").getValue(), "Third element should remain checked=false");
    }

    @Test
    void shouldUpdateMultipleFieldsUsingSameMatchedIndex() {
        // Behavior: Multiple $set operations using $ should all use the same matched index.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-POS-004"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("name", new BsonString("Item1"))
                                .append("price", new BsonInt32(100))
                                .append("quantity", new BsonInt32(1)),
                        new BsonDocument()
                                .append("name", new BsonString("Item2"))
                                .append("price", new BsonInt32(200))
                                .append("quantity", new BsonInt32(2))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Update both price and quantity of the first matching element
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"items.name\": \"Item2\"}", "{\"$set\": {\"items.$.price\": 250, \"items.$.quantity\": 5}}").encode(buf);
            runCommand(channel, buf);
        }

        // Verify
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            updatedDocument = entries.getFirst();
        }

        BsonArray items = BsonHelper.getArray(updatedDocument, "items");

        // First item should be unchanged
        BsonDocument firstItem = items.get(0).asDocument();
        assertEquals(100, BsonHelper.getInteger(firstItem, "price").intValue());
        assertEquals(1, BsonHelper.getInteger(firstItem, "quantity").intValue());

        // Second item (matched) should have both fields updated
        BsonDocument secondItem = items.get(1).asDocument();
        assertEquals(250, BsonHelper.getInteger(secondItem, "price").intValue(), "Price should be updated to 250");
        assertEquals(5, BsonHelper.getInteger(secondItem, "quantity").intValue(), "Quantity should be updated to 5");
    }

    @Test
    void shouldSetElementToNullOnUnsetWithPositionalOperator() {
        // Behavior: $unset with $ positional operator sets the element to null to maintain array stability.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-POS-005"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("name", new BsonString("Keep"))
                                .append("price", new BsonInt32(100)),
                        new BsonDocument()
                                .append("name", new BsonString("Remove"))
                                .append("price", new BsonInt32(200)),
                        new BsonDocument()
                                .append("name", new BsonString("Keep2"))
                                .append("price", new BsonInt32(300))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Unset the price field of the matching element
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"items.name\": \"Remove\"}", "{\"$unset\": {\"items.$.price\": 1}}").encode(buf);
            runCommand(channel, buf);
        }

        // Verify
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            updatedDocument = entries.getFirst();
        }

        BsonArray items = BsonHelper.getArray(updatedDocument, "items");
        assertEquals(3, items.size(), "Array should still have 3 elements");

        // First item - price should remain
        assertTrue(items.get(0).asDocument().containsKey("price"), "First item should still have price");
        assertEquals(100, items.get(0).asDocument().getInt32("price").getValue());

        // Second item - price field should be removed
        assertFalse(items.get(1).asDocument().containsKey("price"), "Second item's price should be removed");
        assertEquals("Remove", BsonHelper.getString(items.get(1).asDocument(), "name"), "Name should remain");

        // Third item - price should remain
        assertTrue(items.get(2).asDocument().containsKey("price"), "Third item should still have price");
        assertEquals(300, items.get(2).asDocument().getInt32("price").getValue());
    }

    @Test
    void shouldUnsetFirstArrayElementWithPositionalOperator() {
        // Behavior: $unset with $ positional operator correctly targets the first element (index 0).

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-UNSET-001"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument().append("name", new BsonString("First")).append("price", new BsonInt32(100)),
                        new BsonDocument().append("name", new BsonString("Second")).append("price", new BsonInt32(200)),
                        new BsonDocument().append("name", new BsonString("Third")).append("price", new BsonInt32(300))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Unset price from first element
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"items.name\": \"First\"}", "{\"$unset\": {\"items.$.price\": 1}}").encode(buf);
        runCommand(channel, buf);

        // Verify
        BsonDocument updatedDoc = queryFirstDocument(cmd);
        assertNotNull(updatedDoc);
        BsonArray items = BsonHelper.getArray(updatedDoc, "items");

        assertFalse(items.get(0).asDocument().containsKey("price"), "First item's price should be removed");
        assertTrue(items.get(1).asDocument().containsKey("price"), "Second item should still have price");
        assertTrue(items.get(2).asDocument().containsKey("price"), "Third item should still have price");
    }

    @Test
    void shouldUnsetLastArrayElementWithPositionalOperator() {
        // Behavior: $unset with $ positional operator correctly targets the last element.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-UNSET-002"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument().append("name", new BsonString("First")).append("price", new BsonInt32(100)),
                        new BsonDocument().append("name", new BsonString("Second")).append("price", new BsonInt32(200)),
                        new BsonDocument().append("name", new BsonString("Third")).append("price", new BsonInt32(300))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Unset price from last element
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"items.name\": \"Third\"}", "{\"$unset\": {\"items.$.price\": 1}}").encode(buf);
        runCommand(channel, buf);

        // Verify
        BsonDocument updatedDoc = queryFirstDocument(cmd);
        assertNotNull(updatedDoc);
        BsonArray items = BsonHelper.getArray(updatedDoc, "items");

        assertTrue(items.get(0).asDocument().containsKey("price"), "First item should still have price");
        assertTrue(items.get(1).asDocument().containsKey("price"), "Second item should still have price");
        assertFalse(items.get(2).asDocument().containsKey("price"), "Third item's price should be removed");
    }

    @Test
    void shouldUnsetWithOrQueryAndPositionalOperator() {
        // Behavior: $unset with $or query and $ operator targets the first matching element in array order.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Array: [shipped, pending] - shipped is at index 0
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-UNSET-003"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument().append("status", new BsonString("shipped")).append("tracking", new BsonString("TRK-001")),
                        new BsonDocument().append("status", new BsonString("pending")).append("tracking", new BsonString("TRK-002"))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Query: pending OR shipped - "pending" is first in query but "shipped" is first in array
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"$or\": [{\"items.status\": \"pending\"}, {\"items.status\": \"shipped\"}]}",
                "{\"$unset\": {\"items.$.tracking\": 1}}").encode(buf);
        runCommand(channel, buf);

        // Verify - index 0 (shipped) should have tracking removed
        BsonDocument updatedDoc = queryFirstDocument(cmd);
        assertNotNull(updatedDoc);
        BsonArray items = BsonHelper.getArray(updatedDoc, "items");

        assertFalse(items.get(0).asDocument().containsKey("tracking"),
                "First item's tracking should be removed (first in array order)");
        assertTrue(items.get(1).asDocument().containsKey("tracking"),
                "Second item should still have tracking");
    }

    @Test
    void shouldUnsetWithElemMatchAndPositionalOperator() {
        // Behavior: $unset with $elemMatch and $ operator targets the first element matching all criteria.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-UNSET-004"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument().append("name", new BsonString("A")).append("price", new BsonInt32(50)).append("discount", new BsonInt32(5)),
                        new BsonDocument().append("name", new BsonString("B")).append("price", new BsonInt32(150)).append("discount", new BsonInt32(10)),
                        new BsonDocument().append("name", new BsonString("C")).append("price", new BsonInt32(200)).append("discount", new BsonInt32(15))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Query with $elemMatch: price >= 100 AND discount >= 10 → matches B (index 1)
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET,
                "{\"items\": {\"$elemMatch\": {\"price\": {\"$gte\": 100}, \"discount\": {\"$gte\": 10}}}}",
                "{\"$unset\": {\"items.$.discount\": 1}}").encode(buf);
        runCommand(channel, buf);

        // Verify
        BsonDocument updatedDoc = queryFirstDocument(cmd);
        assertNotNull(updatedDoc);
        BsonArray items = BsonHelper.getArray(updatedDoc, "items");

        assertTrue(items.get(0).asDocument().containsKey("discount"), "Item A should still have discount");
        assertFalse(items.get(1).asDocument().containsKey("discount"), "Item B's discount should be removed");
        assertTrue(items.get(2).asDocument().containsKey("discount"), "Item C should still have discount");
    }

    @Test
    void shouldUnsetNestedPathWithPositionalOperator() {
        // Behavior: $unset with $ operator works with nested paths (data.items.$.field).

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-UNSET-005"))
                .append("data", new BsonDocument()
                        .append("items", new BsonArray(Arrays.asList(
                                new BsonDocument().append("name", new BsonString("Widget")).append("meta", new BsonString("info1")),
                                new BsonDocument().append("name", new BsonString("Gadget")).append("meta", new BsonString("info2")),
                                new BsonDocument().append("name", new BsonString("Gizmo")).append("meta", new BsonString("info3"))
                        ))));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Unset meta from nested path
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"data.items.name\": \"Gadget\"}", "{\"$unset\": {\"data.items.$.meta\": 1}}").encode(buf);
        runCommand(channel, buf);

        // Verify
        BsonDocument updatedDoc = queryFirstDocument(cmd);
        assertNotNull(updatedDoc);
        BsonDocument data = updatedDoc.getDocument("data");
        BsonArray items = data.getArray("items");

        assertTrue(items.get(0).asDocument().containsKey("meta"), "Widget should still have meta");
        assertFalse(items.get(1).asDocument().containsKey("meta"), "Gadget's meta should be removed");
        assertTrue(items.get(2).asDocument().containsKey("meta"), "Gizmo should still have meta");
    }

    @Test
    void shouldUpdateWithElemMatchAndPositionalOperator() {
        // Behavior: $elemMatch combined with $ positional operator targets the first element
        // satisfying all $elemMatch criteria.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-POS-006"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("name", new BsonString("A"))
                                .append("price", new BsonInt32(50))
                                .append("inStock", new BsonBoolean(true)),
                        new BsonDocument()
                                .append("name", new BsonString("B"))
                                .append("price", new BsonInt32(150))
                                .append("inStock", new BsonBoolean(false)),
                        new BsonDocument()
                                .append("name", new BsonString("C"))
                                .append("price", new BsonInt32(150))
                                .append("inStock", new BsonBoolean(true)),
                        new BsonDocument()
                                .append("name", new BsonString("D"))
                                .append("price", new BsonInt32(200))
                                .append("inStock", new BsonBoolean(true))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Query with $elemMatch: price >= 100 AND inStock = true
        // Should match C (index 2) as first match (B has price >= 100 but inStock=false)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET,
                    "{\"items\": {\"$elemMatch\": {\"price\": {\"$gte\": 100}, \"inStock\": true}}}",
                    "{\"$set\": {\"items.$.processed\": true}}").encode(buf);
            runCommand(channel, buf);
        }

        // Verify
        BsonDocument updatedDocument;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);

            List<BsonDocument> entries = extractEntries(msg);
            updatedDocument = entries.getFirst();
        }

        BsonArray items = BsonHelper.getArray(updatedDocument, "items");

        // A (price=50) - doesn't match $elemMatch, should NOT have processed field
        assertFalse(items.get(0).asDocument().containsKey("processed"), "Item A should not be processed");

        // B (price=150, inStock=false) - doesn't match $elemMatch (inStock false)
        assertFalse(items.get(1).asDocument().containsKey("processed"), "Item B should not be processed");

        // C (price=150, inStock=true) - FIRST element matching $elemMatch, should be processed
        assertTrue(items.get(2).asDocument().containsKey("processed"), "Item C should be processed");
        assertTrue(items.get(2).asDocument().getBoolean("processed").getValue(), "Item C processed should be true");

        // D (price=200, inStock=true) - matches but NOT first, should NOT be processed
        assertFalse(items.get(3).asDocument().containsKey("processed"), "Item D should not be processed (not first match)");
    }

    @Test
    void shouldFailPositionalOperatorWithUpsert() {
        // Behavior: Using $ positional operator with upsert=true should fail because there's no
        // existing document to determine the matched array position.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Try to use $ with upsert - should fail
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"items.status\": \"pending\"}", "{\"$set\": {\"items.$.status\": \"processed\"}, \"upsert\": true}").encode(buf);
        Object msg = runCommand(channel, buf);

        // Should return an error
        assertInstanceOf(ErrorRedisMessage.class, msg, "Should return error when using $ with upsert");
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("$") || errorMessage.content().contains("upsert"),
                "Error should mention positional operator or upsert issue");
    }

    @Test
    void shouldFailMultiplePositionalOperatorsInSamePath() {
        // Behavior: Multiple $ operators in the same path (e.g., a.$.b.$.c) should be rejected.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert a document first
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-POS-008"))
                .append("items", new BsonArray(Collections.singletonList(
                        new BsonDocument().append("nested", new BsonArray(Collections.singletonList(
                                new BsonDocument().append("value", new BsonInt32(1))))))));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Try to use multiple $ operators in path
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"items.nested.value\": 1}", "{\"$set\": {\"items.$.nested.$.value\": 999}}").encode(buf);
        Object msg = runCommand(channel, buf);

        // Should return an error
        assertInstanceOf(ErrorRedisMessage.class, msg, "Should return error when using multiple $ operators");
    }

    @Test
    void shouldFailWhenPositionalOperatorUsedWithoutArrayInQuery() {
        // Behavior: Using $ operator when query doesn't reference the array field should fail.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert a document with array
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-POS-009"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument().append("name", new BsonString("Widget")).append("price", new BsonInt32(100)),
                        new BsonDocument().append("name", new BsonString("Gadget")).append("price", new BsonInt32(200))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Query references orderId (not items), but update uses items.$
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"orderId\": \"ORD-POS-009\"}", "{\"$set\": {\"items.$.price\": 999}}").encode(buf);
        Object msg = runCommand(channel, buf);

        // Should return an error since query doesn't reference the array
        assertInstanceOf(ErrorRedisMessage.class, msg, "Should fail when query doesn't reference the array field");
    }

    @Test
    void shouldFailWhenArrayIsEmptyWithPositionalOperator() {
        // Behavior: Using $ operator when the array is empty should fail.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert a document with empty array
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-POS-010"))
                .append("items", new BsonArray());

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Query references items.name but array is empty
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"items.name\": \"Widget\"}", "{\"$set\": {\"items.$.price\": 999}}").encode(buf);
        Object msg = runCommand(channel, buf);

        // With empty array, no documents should match the query, so result should be empty (not an error)
        List<ObjectId> objectIds = extractObjectIds(msg);
        assertTrue(objectIds.isEmpty(), "No documents should be updated since array is empty and no match");
    }

    @Test
    void shouldFailWhenNoArrayElementMatchesQuery() {
        // Behavior: Using $ operator when no array element matches should fail.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert a document
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-POS-011"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument().append("name", new BsonString("Widget")).append("price", new BsonInt32(100)),
                        new BsonDocument().append("name", new BsonString("Gadget")).append("price", new BsonInt32(200))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Query for name that doesn't exist in array
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"items.name\": \"NonExistent\"}", "{\"$set\": {\"items.$.price\": 999}}").encode(buf);
        Object msg = runCommand(channel, buf);

        // No documents should be updated since no array element matches
        List<ObjectId> objectIds = extractObjectIds(msg);
        assertTrue(objectIds.isEmpty(), "No documents should be updated since no element matches");
    }

    @Test
    void shouldUpdateWithOrQueryAndPositionalOperator() {
        // Behavior: $ operator should work with $or queries by finding the first matching element.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert a document
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-POS-012"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument().append("name", new BsonString("Widget")).append("status", new BsonString("active")),
                        new BsonDocument().append("name", new BsonString("Gadget")).append("status", new BsonString("pending")),
                        new BsonDocument().append("name", new BsonString("Gizmo")).append("status", new BsonString("inactive"))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Update using $or query - should match 'pending' status at index 1
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"$or\": [{\"items.status\": \"pending\"}, {\"items.status\": \"shipped\"}]}",
                "{\"$set\": {\"items.$.status\": \"processed\"}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Verify the update
        BsonDocument updatedDoc = queryFirstDocument(cmd);
        assertNotNull(updatedDoc);
        BsonArray items = BsonHelper.getArray(updatedDoc, "items");
        assertEquals("active", items.get(0).asDocument().getString("status").getValue());
        assertEquals("processed", items.get(1).asDocument().getString("status").getValue());
        assertEquals("inactive", items.get(2).asDocument().getString("status").getValue());
    }

    @Test
    void shouldUpdateFirstMatchInArrayOrderWithOrQuery() {
        // Behavior: Positional operator locks onto the first element in array order that matches any
        // $or branch, regardless of which branch it matches. Array scan order takes priority.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Array: [shipped, pending] - shipped is at index 0
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-OR-001"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument().append("status", new BsonString("shipped")),
                        new BsonDocument().append("status", new BsonString("pending"))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Query: pending OR shipped - "pending" is first in query but "shipped" is first in array
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"$or\": [{\"items.status\": \"pending\"}, {\"items.status\": \"shipped\"}]}",
                "{\"$set\": {\"items.$.status\": \"processed\"}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Index 0 (shipped) should be updated because it's first in array order
        BsonDocument updatedDoc = queryFirstDocument(cmd);
        assertNotNull(updatedDoc);
        BsonArray items = BsonHelper.getArray(updatedDoc, "items");
        assertEquals("processed", items.get(0).asDocument().getString("status").getValue(),
                "Index 0 should be updated (first match in array scan order)");
        assertEquals("pending", items.get(1).asDocument().getString("status").getValue(),
                "Index 1 should remain unchanged");
    }

    @Test
    void shouldUpdateWithPartialOrBranchMatch() {
        // Behavior: In $or, only one branch needs to match. Tests true OR semantics (not AND).

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Array with single element that only matches one branch
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-OR-002"))
                .append("items", new BsonArray(Collections.singletonList(
                        new BsonDocument().append("status", new BsonString("pending"))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Query: pending OR non_existent - only "pending" branch matches
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"$or\": [{\"items.status\": \"pending\"}, {\"items.status\": \"non_existent\"}]}",
                "{\"$set\": {\"items.$.status\": \"processed\"}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Update should succeed - only one branch needs to match
        BsonDocument updatedDoc = queryFirstDocument(cmd);
        assertNotNull(updatedDoc);
        BsonArray items = BsonHelper.getArray(updatedDoc, "items");
        assertEquals("processed", items.get(0).asDocument().getString("status").getValue(),
                "Update should succeed with partial OR match");
    }

    @Test
    void shouldSelectFirstElementWhenMultipleMatchDifferentOrBranches() {
        // Behavior: When multiple elements match different $or branches, the first element
        // in array order that matches ANY branch wins (first-element-wins rule).

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Element at index 0 matches "name=A" branch, element at index 1 matches "status=shipped" branch
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-OR-003"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument().append("name", new BsonString("A")).append("status", new BsonString("active")),
                        new BsonDocument().append("name", new BsonString("B")).append("status", new BsonString("shipped"))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Query: status=shipped OR name=A - both elements match different branches
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"$or\": [{\"items.status\": \"shipped\"}, {\"items.name\": \"A\"}]}",
                "{\"$set\": {\"items.$.status\": \"processed\"}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Index 0 should be updated (first element in array order that matches any branch)
        BsonDocument updatedDoc = queryFirstDocument(cmd);
        assertNotNull(updatedDoc);
        BsonArray items = BsonHelper.getArray(updatedDoc, "items");
        assertEquals("processed", items.get(0).asDocument().getString("status").getValue(),
                "Index 0 should be updated (first-element-wins rule)");
        assertEquals("shipped", items.get(1).asDocument().getString("status").getValue(),
                "Index 1 should remain unchanged");
    }

    @Test
    void shouldSkipElementsWithMissingFieldsInOrQuery() {
        // Behavior: Elements missing the queried field should be skipped; positional operator
        // binds to the first valid match.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // First element has no "status" field, second element has matching status
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-OR-004"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument().append("name", new BsonString("NoStatus")),
                        new BsonDocument().append("status", new BsonString("pending"))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Query: pending OR shipped - first element doesn't have status field
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"$or\": [{\"items.status\": \"pending\"}, {\"items.status\": \"shipped\"}]}",
                "{\"$set\": {\"items.$.status\": \"processed\"}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Index 1 should be updated (first element with matching field)
        BsonDocument updatedDoc = queryFirstDocument(cmd);
        assertNotNull(updatedDoc);
        BsonArray items = BsonHelper.getArray(updatedDoc, "items");
        assertFalse(items.get(0).asDocument().containsKey("status"),
                "Index 0 should not have status field added");
        assertEquals("processed", items.get(1).asDocument().getString("status").getValue(),
                "Index 1 should be updated (first valid match)");
    }

    @Test
    void shouldUpdateNestedArrayWithPositionalOperator() {
        // Behavior: $ operator should work with nested paths (a.b.$.c pattern).

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert a document with nested array inside another object
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-POS-013"))
                .append("data", new BsonDocument()
                        .append("items", new BsonArray(Arrays.asList(
                                new BsonDocument().append("name", new BsonString("Widget")).append("price", new BsonInt32(100)),
                                new BsonDocument().append("name", new BsonString("Gadget")).append("price", new BsonInt32(200)),
                                new BsonDocument().append("name", new BsonString("Gizmo")).append("price", new BsonInt32(300))
                        ))));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Update using nested path - data.items.$.price for item with name "Gadget"
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"data.items.name\": \"Gadget\"}", "{\"$set\": {\"data.items.$.price\": 250}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Verify the update
        BsonDocument updatedDoc = queryFirstDocument(cmd);
        assertNotNull(updatedDoc);
        BsonDocument data = updatedDoc.getDocument("data");
        BsonArray items = data.getArray("items");

        assertEquals(100, items.get(0).asDocument().getInt32("price").getValue());
        assertEquals(250, items.get(1).asDocument().getInt32("price").getValue());
        assertEquals(300, items.get(2).asDocument().getInt32("price").getValue());
    }

    @Test
    void shouldUpdateWithInOperatorInsideElemMatchAndPositionalOperator() {
        // Behavior: $ operator should work with $in inside $elemMatch.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert a document
        BsonDocument document = new BsonDocument()
                .append("orderId", new BsonString("ORD-POS-014"))
                .append("items", new BsonArray(Arrays.asList(
                        new BsonDocument().append("status", new BsonString("shipped")).append("price", new BsonInt32(100)),
                        new BsonDocument().append("status", new BsonString("pending")).append("price", new BsonInt32(200)),
                        new BsonDocument().append("status", new BsonString("delivered")).append("price", new BsonInt32(300))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Update using $elemMatch with $in - match status in ['pending', 'processing'] AND price >= 150
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET,
                "{\"items\": {\"$elemMatch\": {\"status\": {\"$in\": [\"pending\", \"processing\"]}, \"price\": {\"$gte\": 150}}}}",
                "{\"$set\": {\"items.$.status\": \"completed\"}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Verify the update - item at index 1 should be updated
        BsonDocument updatedDoc = queryFirstDocument(cmd);
        assertNotNull(updatedDoc);
        BsonArray items = BsonHelper.getArray(updatedDoc, "items");

        assertEquals("shipped", items.get(0).asDocument().getString("status").getValue());
        assertEquals("completed", items.get(1).asDocument().getString("status").getValue());
        assertEquals("delivered", items.get(2).asDocument().getString("status").getValue());
    }

    @Test
    void shouldUpdateWithNestedElemMatchAndPositionalOperator() {
        // Behavior: Nested $elemMatch (outer array with inner $elemMatch condition) should correctly
        // identify the first matching outer array element, and the $ positional operator should
        // update that element.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Document with orders array, each order has an items array
        BsonDocument document = new BsonDocument()
                .append("customerId", new BsonString("CUST-001"))
                .append("orders", new BsonArray(Arrays.asList(
                        new BsonDocument()
                                .append("orderId", new BsonString("ORD-1"))
                                .append("items", new BsonArray(Arrays.asList(
                                        new BsonDocument().append("name", new BsonString("Widget")).append("qty", new BsonInt32(2)),
                                        new BsonDocument().append("name", new BsonString("Gadget")).append("qty", new BsonInt32(1))
                                ))),
                        new BsonDocument()
                                .append("orderId", new BsonString("ORD-2"))
                                .append("items", new BsonArray(Arrays.asList(
                                        new BsonDocument().append("name", new BsonString("Widget")).append("qty", new BsonInt32(10)),
                                        new BsonDocument().append("name", new BsonString("Gizmo")).append("qty", new BsonInt32(3))
                                ))),
                        new BsonDocument()
                                .append("orderId", new BsonString("ORD-3"))
                                .append("items", new BsonArray(Arrays.asList(
                                        new BsonDocument().append("name", new BsonString("Widget")).append("qty", new BsonInt32(5)),
                                        new BsonDocument().append("name", new BsonString("Tool")).append("qty", new BsonInt32(7))
                                )))
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Query: Find orders where at least one item has name="Widget" AND qty >= 5
        // ORD-1 has Widget with qty=2 (doesn't match)
        // ORD-2 has Widget with qty=10 (matches - first match)
        // ORD-3 has Widget with qty=5 (matches but not first)
        // Update: Mark the first matching order as "shipped"
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET,
                "{\"orders\": {\"$elemMatch\": {\"items\": {\"$elemMatch\": {\"name\": \"Widget\", \"qty\": {\"$gte\": 5}}}}}}",
                "{\"$set\": {\"orders.$.shipped\": true}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Verify the update
        BsonDocument updatedDoc = queryFirstDocument(cmd);
        assertNotNull(updatedDoc);
        BsonArray orders = BsonHelper.getArray(updatedDoc, "orders");

        // ORD-1: Widget qty=2, doesn't match nested $elemMatch, should NOT have shipped
        assertFalse(orders.get(0).asDocument().containsKey("shipped"),
                "ORD-1 should not have shipped field (Widget qty=2 < 5)");

        // ORD-2: Widget qty=10, FIRST match of nested $elemMatch, should have shipped=true
        assertTrue(orders.get(1).asDocument().containsKey("shipped"),
                "ORD-2 should have shipped field (first order with Widget qty >= 5)");
        assertTrue(orders.get(1).asDocument().getBoolean("shipped").getValue(),
                "ORD-2 shipped should be true");

        // ORD-3: Widget qty=5, matches but NOT first, should NOT have shipped
        assertFalse(orders.get(2).asDocument().containsKey("shipped"),
                "ORD-3 should not have shipped field (matches but not first)");
    }

    @Test
    void shouldUpdateScalarArrayWithElemMatchAndPositionalOperator() {
        // Behavior: $elemMatch on scalar arrays (arrays of primitive values) should correctly
        // identify the first matching element, and the $ positional operator should update
        // that specific element.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Document with scalar number array
        BsonDocument document = new BsonDocument()
                .append("studentId", new BsonString("STU-001"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(75),
                        new BsonInt32(82),
                        new BsonInt32(90),
                        new BsonInt32(88)
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Query: Find documents where at least one score >= 85
        // Elements: 75 (no), 82 (no), 90 (yes - first match at index 2), 88 (yes but not first)
        // Update: Set the first matching score to 100
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET,
                "{\"scores\": {\"$elemMatch\": {\"$gte\": 85}}}",
                "{\"$set\": {\"scores.$\": 100}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Verify the update
        BsonDocument updatedDoc = queryFirstDocument(cmd);
        assertNotNull(updatedDoc);
        BsonArray scores = BsonHelper.getArray(updatedDoc, "scores");

        assertEquals(75, scores.get(0).asInt32().getValue(), "Index 0 should remain 75");
        assertEquals(82, scores.get(1).asInt32().getValue(), "Index 1 should remain 82");
        assertEquals(100, scores.get(2).asInt32().getValue(), "Index 2 should be updated to 100 (first match >= 85)");
        assertEquals(88, scores.get(3).asInt32().getValue(), "Index 3 should remain 88 (matches but not first)");
    }

    @Test
    void shouldUpdateScalarStringArrayWithElemMatchAndPositionalOperator() {
        // Behavior: $elemMatch on scalar string arrays should correctly identify the first
        // matching element, and the $ positional operator should update that specific element.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Document with scalar string array
        BsonDocument document = new BsonDocument()
                .append("productId", new BsonString("PROD-001"))
                .append("tags", new BsonArray(Arrays.asList(
                        new BsonString("electronics"),
                        new BsonString("sale"),
                        new BsonString("featured"),
                        new BsonString("new")
                )));

        List<byte[]> testDocuments = Collections.singletonList(BSONUtil.toBytes(document));
        insertDocumentsAndGetObjectIds(testDocuments);

        // Query: Find documents where at least one tag equals "featured"
        // Update: Replace that tag with "promoted"
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET,
                "{\"tags\": {\"$elemMatch\": {\"$eq\": \"featured\"}}}",
                "{\"$set\": {\"tags.$\": \"promoted\"}}").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);

        // Verify the update
        BsonDocument updatedDoc = queryFirstDocument(cmd);
        assertNotNull(updatedDoc);
        BsonArray tags = BsonHelper.getArray(updatedDoc, "tags");

        assertEquals("electronics", tags.get(0).asString().getValue(), "Index 0 should remain 'electronics'");
        assertEquals("sale", tags.get(1).asString().getValue(), "Index 1 should remain 'sale'");
        assertEquals("promoted", tags.get(2).asString().getValue(), "Index 2 should be updated to 'promoted'");
        assertEquals("new", tags.get(3).asString().getValue(), "Index 3 should remain 'new'");
    }

    @Test
    void shouldNotModifyDocumentsWithoutTargetArrayWhenUsingArrayFilters() {
        // Behavior: UPDATE with $[identifier] and array_filters must only modify documents that
        // contain the target array. Documents without the array (or where the field is not an
        // array) are left unchanged; no literal "$[identifier]" key is created.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Document A has a scores array, document B does not
        BsonDocument docWithArray = new BsonDocument()
                .append("name", new BsonString("Henry"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(75), new BsonInt32(85), new BsonInt32(95)
                )));
        BsonDocument docWithoutArray = new BsonDocument()
                .append("name", new BsonString("Eve"));

        List<byte[]> testDocuments = List.of(BSONUtil.toBytes(docWithArray), BSONUtil.toBytes(docWithoutArray));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(2, insertedDocs.size(), "Should have inserted 2 documents");

        // Set scores >= 80 to 100 across all documents
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}",
                    "{\"$set\": {\"scores.$[elem]\": 100}, \"array_filters\": [{\"elem\": {\"$gte\": 80}}]}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
        }

        // Query and verify both documents
        List<BsonDocument> entries;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            entries = extractEntries(msg);
        }
        assertEquals(2, entries.size(), "Should retrieve exactly 2 documents");

        BsonDocument henry = null;
        BsonDocument eve = null;
        for (BsonDocument entry : entries) {
            String name = BsonHelper.getString(entry, "name");
            if ("Henry".equals(name)) {
                henry = entry;
            } else if ("Eve".equals(name)) {
                eve = entry;
            }
        }

        // Document A: matching elements updated
        assertNotNull(henry, "Henry should be retrieved");
        BsonArray scores = BsonHelper.getArray(henry, "scores");
        assertNotNull(scores, "Scores array should exist");
        assertEquals(3, scores.size(), "Scores array should have 3 elements");
        assertEquals(75, scores.get(0).asInt32().getValue(), "Element below the filter should remain unchanged");
        assertEquals(100, scores.get(1).asInt32().getValue(), "Element matching the filter should be updated");
        assertEquals(100, scores.get(2).asInt32().getValue(), "Element matching the filter should be updated");

        // Document B: untouched, no literal structure created
        assertNotNull(eve, "Eve should be retrieved");
        assertFalse(eve.containsKey("scores"), "No 'scores' field should be created on the document without the array");
    }

    @Test
    void shouldExcludeNoOpDocumentsFromObjectIdsWhenPositionalTargetIsMissing() {
        // Behavior: UPDATE returns only the object_ids of documents it actually modified. A matched
        // document where the positional $set was a no-op because the target array is missing is
        // left untouched and its id does not appear in object_ids.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Document A has a scores array, document B does not
        BsonDocument docWithArray = new BsonDocument()
                .append("name", new BsonString("Henry"))
                .append("scores", new BsonArray(Arrays.asList(
                        new BsonInt32(75), new BsonInt32(85), new BsonInt32(95)
                )));
        BsonDocument docWithoutArray = new BsonDocument()
                .append("name", new BsonString("Eve"));

        List<byte[]> testDocuments = List.of(BSONUtil.toBytes(docWithArray), BSONUtil.toBytes(docWithoutArray));
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(2, insertedDocs.size(), "Should have inserted 2 documents");

        ObjectId henryId = null;
        ObjectId eveId = null;
        for (Map.Entry<ObjectId, byte[]> entry : insertedDocs.entrySet()) {
            BsonDocument doc = BSONUtil.fromBson(entry.getValue());
            if ("Henry".equals(doc.getString("name").getValue())) {
                henryId = entry.getKey();
            } else {
                eveId = entry.getKey();
            }
        }
        assertNotNull(henryId, "Henry's ObjectId should be resolved");
        assertNotNull(eveId, "Eve's ObjectId should be resolved");

        List<ObjectId> updatedObjectIds;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}",
                    "{\"$set\": {\"scores.$[elem]\": 100}, \"array_filters\": [{\"elem\": {\"$gte\": 80}}]}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            updatedObjectIds = extractObjectIds(msg);
        }

        // Only the actually modified document is reported
        assertEquals(1, updatedObjectIds.size(), "object_ids should contain only the modified document");
        assertTrue(updatedObjectIds.contains(henryId), "object_ids should contain the modified document");
        assertFalse(updatedObjectIds.contains(eveId), "object_ids should not contain the no-op document");

        // The no-op document's content is unchanged
        List<BsonDocument> entries;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"name\": \"Eve\"}").encode(buf);
            Object msg = runCommand(channel, buf);
            entries = extractEntries(msg);
        }
        assertEquals(1, entries.size(), "Eve should be retrieved");
        BsonDocument eve = entries.getFirst();
        assertFalse(eve.containsKey("scores"), "No 'scores' field should be created on the no-op document");
    }

    private BsonDocument queryFirstDocument(BucketCommandBuilder<String, String> cmd) {
        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}").encode(buf);
        Object msg = runCommand(channel, buf);
        if (!(msg instanceof MapRedisMessage)) {
            return null;
        }

        List<BsonDocument> entries = extractEntries(msg);
        if (entries.isEmpty()) {
            return null;
        }
        return entries.getFirst();
    }
}
