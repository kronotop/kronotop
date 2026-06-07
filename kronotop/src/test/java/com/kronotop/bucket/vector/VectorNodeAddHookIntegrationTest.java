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

package com.kronotop.bucket.vector;

import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.RESPVersion;
import io.github.jbellis.jvector.graph.SearchResult;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VectorNodeAddHookIntegrationTest extends BaseVectorHookIntegrationTest {

    @Test
    void shouldAddNewVectorToOnHeapGraphAfterUpdate() throws IOException {
        // Behavior: After updating a vector field, the new vector is added to the on-heap graph
        // and becomes searchable. An anchor document ensures graph connectivity.
        createBucketWithVectorIndex();

        float[] anchorVector = {0.9f, 0.9f, 0.9f};
        insertDocumentWithVector(anchorVector, "anchor");

        float[] originalVector = {0.2f, 0.3f, 0.4f};
        ObjectId targetId = insertDocumentWithVector(originalVector, "target");

        try (OnHeapVectorGraphIndex ignored = awaitVectorGraph(TEST_BUCKET, "embedding", 2)) {

            // Update the target document's vector field
            BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
            switchProtocol(cmd, RESPVersion.RESP3);

            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"label\": \"target\"}", "{\"$set\": {\"embedding\": [0.5, 0.6, 0.7]}}").encode(buf);
            Object response = runCommand(channel, buf);

            List<ObjectId> updatedIds = extractObjectIds(response);
            assertEquals(1, updatedIds.size());

            // anchor (alive) + old target (deleted) + new target (alive) = 3 nodes
            try (OnHeapVectorGraphIndex graph = awaitVectorGraph(TEST_BUCKET, "embedding", 3)) {
                // VectorNodeAddHook re-maps the objectId to the new ordinal
                int newOrdinal = graph.getMetadata().findOrdinal(targetId);
                assertTrue(newOrdinal >= 0, "Updated objectId should map to a valid ordinal");

                // The new vector should be searchable
                float[] newVector = {0.5f, 0.6f, 0.7f};
                SearchResult result = graph.search(newVector, 2);
                assertTrue(result.getNodes().length >= 1, "New vector should be searchable after update");
            }
        }
    }

    @Test
    void shouldAddVectorToOnHeapGraphAfterUpsert() throws IOException {
        // Behavior: When an UPSERT inserts a new document with a vector field, the vector is added
        // to the on-heap graph.
        createBucketWithVectorIndex();

        // Upsert a document with a vector — filter matches nothing, so a new doc is inserted
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"label\": \"nonexistent\"}",
                "{\"$set\": {\"embedding\": [0.5, 0.6, 0.7]}, \"upsert\": true}").encode(buf);
        Object response = runCommand(channel, buf);

        List<ObjectId> upsertedIds = extractObjectIds(response);
        assertEquals(1, upsertedIds.size());

        try (OnHeapVectorGraphIndex graph = awaitVectorGraph(TEST_BUCKET, "embedding", 1)) {
            // Search for the upserted vector
            float[] vector = {0.5f, 0.6f, 0.7f};
            SearchResult result = graph.search(vector, 1);
            assertEquals(1, result.getNodes().length, "Upserted vector should be searchable");
        }
    }

    @Test
    void shouldAddNewVectorToOnHeapGraphAfterUpdateMultipleDocs() throws IOException {
        // Behavior: After updating the vector field on multiple documents, all new vectors are
        // added to the on-heap graph. An anchor document ensures graph connectivity.
        createBucketWithVectorIndex();

        float[] anchorVector = {0.9f, 0.9f, 0.9f};
        insertDocumentWithVector(anchorVector, "anchor");

        float[] vector1 = {0.1f, 0.2f, 0.3f};
        float[] vector2 = {0.4f, 0.5f, 0.6f};
        ObjectId target1Id = insertDocumentWithVector(vector1, "target");
        ObjectId target2Id = insertDocumentWithVector(vector2, "target");

        try (OnHeapVectorGraphIndex ignored = awaitVectorGraph(TEST_BUCKET, "embedding", 3)) {

            // Update both target docs
            BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
            switchProtocol(cmd, RESPVersion.RESP3);

            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"label\": \"target\"}", "{\"$set\": {\"embedding\": [0.8, 0.7, 0.6]}}").encode(buf);
            Object response = runCommand(channel, buf);

            List<ObjectId> updatedIds = extractObjectIds(response);
            assertEquals(2, updatedIds.size());

            // anchor (alive) + 2 old targets (deleted) + 2 new targets (alive) = 5 nodes
            try (OnHeapVectorGraphIndex graph = awaitVectorGraph(TEST_BUCKET, "embedding", 5)) {
                // VectorNodeAddHook re-maps both objectIds to new ordinals
                assertTrue(graph.getMetadata().findOrdinal(target1Id) >= 0,
                        "First updated objectId should map to a valid ordinal");
                assertTrue(graph.getMetadata().findOrdinal(target2Id) >= 0,
                        "Second updated objectId should map to a valid ordinal");

                // The new vector should be searchable
                float[] newVector = {0.8f, 0.7f, 0.6f};
                SearchResult result = graph.search(newVector, 3);
                assertTrue(result.getNodes().length >= 1, "New vectors should be searchable after multi-doc update");
            }
        }
    }
}
