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

import com.apple.foundationdb.tuple.Versionstamp;

import java.util.*;

public class BaseExecutor {
    PipelineNode findHeadNode(PipelineNode node) {
        if (Objects.isNull(node)) {
            return null;
        }

        PipelineNode head = node;
        while (head.next() != null) {
            head = node.next();
        }
        return head;
    }

    Map<Integer, List<Versionstamp>> accumulateAndGroupVersionstampsByShardId(DataSink sink) {
        Map<Integer, List<Versionstamp>> byShardId = new HashMap<>();
        switch (sink) {
            case PersistedEntrySink persistedEntrySink -> persistedEntrySink.forEach((versionstamp, entry) -> {
                byShardId.compute(entry.shardId(), (k, versionstamps) -> {
                    if (versionstamps == null) {
                        versionstamps = new ArrayList<>();
                    }
                    versionstamps.add(versionstamp);
                    return versionstamps;
                });
            });
            case DocumentLocationSink documentLocationSink -> documentLocationSink.forEach((ignored, entry) -> {
                byShardId.compute(entry.shardId(), (k, versionstamps) -> {
                    if (versionstamps == null) {
                        versionstamps = new ArrayList<>();
                    }
                    versionstamps.add(entry.versionstamp());
                    return versionstamps;
                });
            });
        }
        return byShardId;
    }
}
