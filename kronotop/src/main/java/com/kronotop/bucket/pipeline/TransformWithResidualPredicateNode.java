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

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Pipeline node that applies residual predicates to filter documents after index retrieval.
 * Residual predicates are query conditions that cannot be evaluated using indexes alone,
 * such as $elemMatch operators or conditions on non-indexed fields.
 *
 * <p>This node processes documents from its parent sink and writes only those that
 * satisfy the residual predicate to a new sink. It handles two sink types:
 * <ul>
 *   <li>{@link PersistedEntrySink} - documents already in memory, filtered directly</li>
 *   <li>{@link DocumentLocationSink} - document locations requiring batch retrieval before filtering</li>
 * </ul>
 */
public class TransformWithResidualPredicateNode extends AbstractPipelineNode implements TransformationNode {
    private final ResidualPredicateNode residualPredicate;

    public TransformWithResidualPredicateNode(int id, ResidualPredicateNode residualPredicate) {
        super(id);
        this.residualPredicate = residualPredicate;
    }

    public ResidualPredicateNode predicate() {
        return residualPredicate;
    }

    @Override
    public void transform(QueryContext ctx) {
        int parentId = ctx.getParentId(id());

        DataSink sink = ctx.sinks().load(parentId);
        if (sink == null) {
            return;
        }

        PersistedEntrySink newSink = ctx.sinks().loadOrCreatePersistedEntrySink(id());
        DocumentView view = new DocumentView();
        try {
            switch (sink) {
                case PersistedEntrySink persistedEntrySink -> {
                    for (PersistedEntry entry : persistedEntrySink.entries()) {
                        view.reset(entry.objectId(), entry.document());
                        if (residualPredicate.test(view, ctx.getParameters(), ctx.env().collatorCache())) {
                            newSink.append(entry);
                        }
                    }
                }
                case DocumentLocationSink documentLocationSink -> {
                    // Deduplicate by ObjectId only when a multi-key index produced the results
                    if (ctx.isScannedIndexMultiKey()) {
                        documentLocationSink.dedupByObjectId();
                    }

                    if (documentLocationSink.size() == 0) {
                        break;
                    }

                    // Phase 2: Batch retrieve all documents
                    List<ByteBuffer> documents = ctx.env().documentRetriever()
                            .retrieveDocuments(documentLocationSink.entries());

                    // Phase 3: Filter and write
                    for (int i = 0; i < documents.size(); i++) {
                        ByteBuffer document = documents.get(i);
                        DocumentLocation location = documentLocationSink.entries().get(i);
                        view.reset(location.objectId(), document);
                        if (residualPredicate.test(view, ctx.getParameters(), ctx.env().collatorCache())) {
                            PersistedEntry entry = new PersistedEntry(
                                    location.objectId(),
                                    location.shardId(),
                                    location.entryMetadata(),
                                    document,
                                    location.cursorIndexValue()
                            );
                            newSink.append(entry);
                        }
                    }
                }
                default -> throw new IllegalStateException("Unexpected value: " + sink);
            }
        } finally {
            sink.clear();
        }
    }
}
