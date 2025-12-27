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

package com.kronotop.bucket.pipeline;

import java.nio.ByteBuffer;

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

        DataSink newSink = ctx.sinks().loadOrCreatePersistedEntrySink(id());
        try {
            switch (sink) {
                case PersistedEntrySink persistedEntrySink -> {
                    persistedEntrySink.forEach(((versionstamp, entry) -> {
                        if (residualPredicate.test(entry.document())) {
                            ctx.sinks().writePersistedEntry(newSink, versionstamp, entry);
                        }
                    }));
                }
                case DocumentLocationSink documentLocationSink -> {
                    documentLocationSink.forEach((entryHandle, location) -> {
                        ByteBuffer document = ctx.env().documentRetriever().retrieveDocument(ctx.metadata(), location);
                        if (residualPredicate.test(document)) {
                            PersistedEntry entry = new PersistedEntry(location.shardId(), entryHandle, document);
                            ctx.sinks().writePersistedEntry(newSink, location.versionstamp(), entry);
                        }
                    });
                }
                default -> throw new IllegalStateException("Unexpected value: " + sink);
            }
        } finally {
            sink.clear();
        }
    }
}
