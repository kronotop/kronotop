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

import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.index.*;
import com.kronotop.internal.JSONUtil;
import org.bson.BsonType;
import tools.jackson.databind.DatabindException;

import java.util.List;
import java.util.Map;

/**
 * Shared utility for index creation logic used by both bucket creation and index create subcommand.
 */
class IndexCreationHelper {

    static IndexSchemaPayload deserializeAndValidate(byte[] data) {
        try {
            IndexSchemaPayload payload = JSONUtil.readValue(data, IndexSchemaPayload.class);
            boolean hasSingleField = !payload.getSingleFieldSchemas().isEmpty();
            boolean hasCompound = payload.getCompoundSchemas() != null && !payload.getCompoundSchemas().isEmpty();
            boolean hasVector = payload.getVectorSchema() != null;
            if (!hasSingleField && !hasCompound && !hasVector) {
                throw new KronotopException("Index schemas cannot be empty");
            }
            return payload;
        } catch (KronotopException e) {
            if (e.getCause() instanceof DatabindException jsonException) {
                if (jsonException.getCause() instanceof IllegalArgumentException illegalArgumentException) {
                    throw new KronotopException(illegalArgumentException);
                }
                throw new KronotopException("Invalid index schema");
            }
            throw e;
        }
    }

    static void createIndexes(TransactionalContext tx, BucketMetadata metadata, IndexSchemaPayload payload, IndexStatus initialStatus) {
        // Create single-field indexes
        for (Map.Entry<String, IndexSchema> entry : payload.getSingleFieldSchemas().entrySet()) {
            IndexSchema schema = entry.getValue();
            schema.validate();

            String name = schema.getName();
            if (name == null) {
                name = IndexNameGenerator.generate(entry.getKey(), schema.getBsonType());
            }
            Collation collation = schema.getCollation();
            if (collation == null && schema.getBsonType() == BsonType.STRING) {
                collation = metadata.collation();
            }

            SingleFieldIndexDefinition indexDefinition = SingleFieldIndexDefinition.create(
                    name,
                    entry.getKey(),
                    schema.getBsonType(),
                    schema.getMultiKey(),
                    initialStatus,
                    collation
            );

            if (metadata.indexes().getIndex(entry.getKey(), IndexSelectionPolicy.ALL) != null) {
                throw new KronotopException("An index on field '" + entry.getKey() + "' already exists");
            }

            SingleFieldIndexUtil.create(tx, metadata, indexDefinition);
        }

        // Create compound indexes
        if (payload.getCompoundSchemas() != null) {
            for (CompoundIndexSchema compoundSchema : payload.getCompoundSchemas()) {
                compoundSchema.validate();

                List<CompoundIndexField> fields = compoundSchema.getFields().stream()
                        .map(f -> new CompoundIndexField(f.getSelector(), f.getBsonType(), f.getMultiKey()))
                        .toList();

                String name = compoundSchema.getName();
                if (name == null) {
                    name = CompoundIndexNameGenerator.generate(fields);
                }

                Collation collation = compoundSchema.getCollation();
                if (collation == null) {
                    boolean hasStringField = compoundSchema.getFields().stream()
                            .anyMatch(f -> f.getBsonType() == BsonType.STRING);
                    if (hasStringField) {
                        collation = metadata.collation();
                    }
                }

                for (CompoundIndex existing : metadata.compoundIndexes().getIndexes(IndexSelectionPolicy.ALL)) {
                    if (existing.definition().fields().equals(fields)) {
                        throw new KronotopException(
                                "A compound index on the same fields already exists: '" + existing.definition().name() + "'"
                        );
                    }
                }

                CompoundIndexDefinition definition = CompoundIndexDefinition.create(name, fields, initialStatus, collation);
                CompoundIndexUtil.create(tx, metadata, definition);
            }
        }

        // Create vector index
        if (payload.getVectorSchema() != null) {
            if (metadata.shards().size() > 1) {
                throw new KronotopException("Vector indexes require single-shard buckets");
            }
            VectorIndexSchema vectorSchema = payload.getVectorSchema();
            vectorSchema.validate();

            String name = vectorSchema.getName();
            if (name == null) {
                name = VectorIndexNameGenerator.generate(vectorSchema.getField(), vectorSchema.getDimensions(), vectorSchema.getDistance());
            }

            if (metadata.vectorIndexes().getIndexBySelector(vectorSchema.getField(), IndexSelectionPolicy.ALL) != null) {
                throw new KronotopException("A vector index on field '" + vectorSchema.getField() + "' already exists");
            }

            VectorIndexDefinition definition = VectorIndexDefinition.create(
                    name,
                    vectorSchema.getField(),
                    vectorSchema.getDimensions(),
                    vectorSchema.getDistance(),
                    initialStatus
            );
            VectorIndexUtil.create(tx, metadata, definition);
        }
    }
}
