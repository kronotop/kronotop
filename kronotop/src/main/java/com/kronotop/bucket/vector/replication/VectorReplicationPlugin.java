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

package com.kronotop.bucket.vector.replication;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.vector.CollectedVector;
import com.kronotop.bucket.vector.VectorNodeWriter;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.replication.ReplicationHintEntry;
import com.kronotop.volume.replication.ReplicationHintPlugin;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public final class VectorReplicationPlugin implements ReplicationHintPlugin {
    private static final Logger LOGGER = LoggerFactory.getLogger(VectorReplicationPlugin.class);

    private final Context context;
    private final BucketService service;

    public VectorReplicationPlugin(BucketService service) {
        this.context = service.getContext();
        this.service = service;
    }

    private void processVectorNodeAdd(ReplicationHintEntry hint, Versionstamp addVs, ByteBuffer document) {
        try (Transaction tr = context.getTransactionManager().create()) {
            BucketMetadata metadata = BucketMetadataUtil.openByPrefix(context, tr, hint.prefix());
            VectorNodeWriter writer = new VectorNodeWriter(service, metadata);
            VectorIndex index = metadata.vectorIndexes().getIndexById(hint.indexId(), IndexSelectionPolicy.WRITABLE);
            if (index == null) {
                LOGGER.warn("Vector index not found, skipping replication hint: indexId={}, prefix={}, segmentId={}, position={}",
                        hint.indexId(), hint.prefix(), hint.segmentId(), hint.position());
                return;
            }
            EntryMetadata entryMetadata = new EntryMetadata(
                    hint.segmentId(),
                    hint.prefix(),
                    hint.position(),
                    hint.length(),
                    EntryMetadata.STUB_HANDLE // not useful for vector processing.
            );
            BsonValue id = SelectorMatcher.match(PrimaryIndex.SELECTOR, document);
            float[] vector = VectorIndexMaintainer.extractVector(index.definition(), document);
            if (vector == null) {
                LOGGER.warn("Vector field not found in document, skipping replication hint: indexId={}, selector={}, id={}, segmentId={}, position={}",
                        hint.indexId(), index.definition().selector(), id, hint.segmentId(), hint.position());
                return;
            }
            CollectedVector cv = new CollectedVector(
                    id.asObjectId().getValue(),
                    metadata.shards().getFirst(),
                    entryMetadata,
                    vector,
                    index.definition(),
                    addVs.getUserVersion()
            );
            writer.write(cv, addVs);
        }
    }

    private void processVectorNodeDelete(ReplicationHintEntry hint, Versionstamp addVs, ByteBuffer document) {
    }

    @Override
    public void process(ReplicationHintEntry hint, Versionstamp addVs, ByteBuffer document) {
        try {
            switch (hint.kind()) {
                case VECTOR_NODE_ADD -> processVectorNodeAdd(hint, addVs, document);
                case VECTOR_NODE_DELETE -> processVectorNodeDelete(hint, addVs, document);
                default -> throw new IllegalArgumentException("Unknown ReplicationHintEntry: " + hint);
            }
        } catch (Exception exp) {
            LOGGER.error("Failed to process replication hint: kind={}, indexId={}, prefix={}, segmentId={}, position={}",
                    hint.kind(), hint.indexId(), hint.prefix(), hint.segmentId(), hint.position(), exp);
        }
    }
}
