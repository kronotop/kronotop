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

import java.util.*;

public class BaseExecutor {
    PipelineNode findHeadNode(PipelineNode node) {
        if (Objects.isNull(node)) {
            return null;
        }

        PipelineNode head = node;
        while (head.next() != null) {
            head = head.next();
        }
        return head;
    }

    Map<Integer, List<DocumentRef>> accumulateDocumentRefsByShardId(QueryContext ctx, DataSink sink) {
        // Deduplicate by ObjectId only when a multi-key index produced the results
        if (ctx.isScannedIndexMultiKey()) {
            sink.dedupByObjectId();
        }

        Map<Integer, List<DocumentRef>> byShardId = new HashMap<>();
        for (DocumentRef ref : sink.entries()) {
            byShardId.compute(ref.shardId(), (k, refs) -> {
                if (refs == null) {
                    refs = new ArrayList<>();
                }
                refs.add(ref);
                return refs;
            });
        }
        return byShardId;
    }
}
