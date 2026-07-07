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

import java.util.List;
import java.util.Objects;

public abstract class AbstractLogicalNode extends AbstractPipelineNode implements LogicalNode {
    private final List<PipelineNode> children;

    public AbstractLogicalNode(int id, List<PipelineNode> children) {
        super(id);
        this.children = children;
    }

    @Override
    public List<PipelineNode> children() {
        return children;
    }

    /**
     * Walks the {@code next()} chain from the given node and returns the last node,
     * the one whose {@code next()} is {@code null}.
     *
     * @param node the node to start from
     * @return the last node in the chain, or {@code null} if {@code node} is {@code null}
     */
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
