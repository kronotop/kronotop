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

import java.util.ArrayList;
import java.util.List;

/**
 * Executes a materialized scan pipeline and collects persisted entries.
 * Used for post-filter vector search where candidates come from an external supplier
 * and are filtered through the standard pipeline.
 */
public final class MaterializedExecutor extends BaseExecutor {
    private final PipelineExecutor executor;

    public MaterializedExecutor(PipelineExecutor executor) {
        this.executor = executor;
    }

    public List<PersistedEntry> execute(QueryContext ctx) {
        executor.execute(null, ctx);

        DataSink sink = ctx.sinks().load(ctx.plan().id());
        if (sink == null) {
            return List.of();
        }

        try {
            List<PersistedEntry> result = new ArrayList<>(sink.size());
            for (DocumentRef ref : sink.entries()) {
                result.add((PersistedEntry) ref);
            }
            return result;
        } finally {
            sink.clear();
        }
    }
}
