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

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.IndexDefinition;
import org.bson.types.ObjectId;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A scan node that pulls document candidates from a {@link CandidateSupplier} on demand.
 * <p>
 * Used when candidates are produced externally (e.g., vector similarity search)
 * and need to be filtered through the standard pipeline. The supplier delivers
 * candidates in batches; each {@link #execute} call processes one batch before
 * requesting the next.
 */
public class MaterializedScanNode extends AbstractPipelineNode implements ScanNode {
    private final CandidateSupplier supplier;
    private final ResidualPredicateNode predicate;
    private List<DocumentLocation> buffer;
    private int offset;
    private boolean supplierExhausted;

    public MaterializedScanNode(int id, CandidateSupplier supplier, ResidualPredicateNode predicate) {
        super(id);
        this.supplier = supplier;
        this.predicate = predicate;
        this.buffer = List.of();
    }

    @Override
    public IndexDefinition getIndexDefinition() {
        return null;
    }

    @Override
    public void execute(QueryContext ctx, Transaction tr) {
        ExecutionState state = ctx.getOrCreateExecutionState(id());
        int limit = state.getLimit();

        // Refill buffer when the current batch is consumed
        if (offset >= buffer.size()) {
            if (supplierExhausted) {
                state.setExhausted(true);
                return;
            }
            buffer = supplier.fetch();
            offset = 0;
            if (buffer.isEmpty()) {
                supplierExhausted = true;
                state.setExhausted(true);
                return;
            }
        }

        // Calculate batch from current offset
        int remaining = buffer.size() - offset;
        int batchSize = (limit > 0) ? Math.min(limit, remaining) : remaining;
        List<DocumentLocation> batch = buffer.subList(offset, offset + batchSize);

        // Retrieve documents from disk
        List<ByteBuffer> documents = ctx.env().documentRetriever().retrieveDocuments(batch);

        // Filter and write results
        PersistedEntrySink sink = ctx.sinks().loadOrCreatePersistedEntrySink(id());
        List<BqlValue> parameters = ctx.getParameters();
        DocumentView view = new DocumentView();

        for (int i = 0; i < documents.size(); i++) {
            ByteBuffer document = documents.get(i);
            DocumentLocation location = batch.get(i);
            ObjectId objectId = location.objectId();

            view.reset(objectId, document);
            if (predicate.test(view, parameters, ctx.env().collatorCache())) {
                PersistedEntry entry = new PersistedEntry(
                        objectId,
                        location.shardId(),
                        location.entryMetadata(),
                        document
                );
                sink.append(entry);
            }
        }

        offset += batchSize;
        if (offset >= buffer.size() && supplierExhausted) {
            state.setExhausted(true);
        }
    }
}
