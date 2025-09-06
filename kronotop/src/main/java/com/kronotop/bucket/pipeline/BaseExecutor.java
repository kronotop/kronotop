package com.kronotop.bucket.pipeline;

import java.util.Objects;

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
}
