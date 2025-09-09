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
