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

import static org.junit.jupiter.api.Assertions.*;

class VectorNodeDeleteHookIntegrationTest extends BaseVectorHookIntegrationTest {

    @Test
    void shouldMarkNodeDeletedOnHeapAfterDelete() throws IOException {
        // Behavior: After deleting a document with a vector field, the on-heap graph marks
        // the deleted node so that its mapping is removed and it no longer appears in search results.
        createBucketWithVectorIndex();

        float[] vector1 = {0.2f, 0.3f, 0.4f};
        float[] vector2 = {0.7f, 0.8f, 0.9f};
        insertDocumentWithVector(vector1, "first");
        insertDocumentWithVector(vector2, "second");

        try (OnHeapVectorGraphIndex graph = awaitVectorGraph(TEST_BUCKET, "embedding", 2)) {

            // Delete the first document by filter
            BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
            switchProtocol(cmd, RESPVersion.RESP3);

            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"label\": \"first\"}").encode(buf);
            Object response = runCommand(channel, buf);

            List<ObjectId> deletedIds = extractObjectIds(response);
            assertEquals(1, deletedIds.size());

            // The deleted node's mapping should be removed from metadata
            ObjectId deletedId = deletedIds.getFirst();
            assertEquals(-1, graph.getMetadata().findOrdinal(deletedId));

            // The remaining document should still be searchable
            SearchResult remainingSearch = graph.search(vector2, 2);
            assertEquals(1, remainingSearch.getNodes().length, "Remaining node should still be searchable");
        }
    }

    @Test
    void shouldMarkNodeDeletedOnHeapAfterUpdateVectorField() throws IOException {
        // Behavior: After updating the vector field of a document, the old vector node is marked
        // deleted on the on-heap graph. The VectorNodeAddHook then re-maps the objectId to a new
        // ordinal for the updated vector, so findOrdinal returns the new ordinal (not -1).
        createBucketWithVectorIndex();

        float[] originalVector = {0.2f, 0.3f, 0.4f};
        insertDocumentWithVector(originalVector, "test");

        try (OnHeapVectorGraphIndex ignored = awaitVectorGraph(TEST_BUCKET, "embedding", 1)) {

            // Update the vector field
            BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
            switchProtocol(cmd, RESPVersion.RESP3);

            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"embedding\": [0.5, 0.6, 0.7]}}").encode(buf);
            Object response = runCommand(channel, buf);

            List<ObjectId> updatedIds = extractObjectIds(response);
            assertEquals(1, updatedIds.size());

            ObjectId updatedId = updatedIds.getFirst();

            // After update: delete hook removes old mapping, add hook creates new mapping.
            // The objectId now maps to a new ordinal (the newly added node).
            try (OnHeapVectorGraphIndex graph = awaitVectorGraph(TEST_BUCKET, "embedding", 2)) {
                int newOrdinal = graph.getMetadata().findOrdinal(updatedId);
                assertTrue(newOrdinal >= 0, "ObjectId should map to a new ordinal after vector update");

                // The old ordinal's entry metadata should be removed by delete hook
                assertNull(graph.getMetadata().findDocumentLocation(0),
                        "Old ordinal's metadata should be removed by delete hook");
            }
        }
    }

    @Test
    void shouldNotAffectOnHeapGraphWhenNonVectorFieldUpdated() throws IOException {
        // Behavior: Updating a non-vector field does not affect the on-heap vector graph;
        // the existing vector node remains searchable.
        createBucketWithVectorIndex();

        float[] vector = {0.2f, 0.3f, 0.4f};
        ObjectId insertedId = insertDocumentWithVector(vector, "original");

        try (OnHeapVectorGraphIndex graph = awaitVectorGraph(TEST_BUCKET, "embedding", 1)) {

            // Update a non-vector field
            BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
            switchProtocol(cmd, RESPVersion.RESP3);

            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"label\": \"updated\"}}").encode(buf);
            Object response = runCommand(channel, buf);

            List<ObjectId> updatedIds = extractObjectIds(response);
            assertEquals(1, updatedIds.size());

            // Graph should still have exactly 1 node
            assertEquals(1, graph.size());

            // The mapping should still exist
            assertTrue(graph.getMetadata().findOrdinal(insertedId) >= 0);

            // The vector should still be searchable
            SearchResult result = graph.search(vector, 1);
            assertEquals(1, result.getNodes().length, "Vector should still be searchable after non-vector field update");
        }
    }
}
