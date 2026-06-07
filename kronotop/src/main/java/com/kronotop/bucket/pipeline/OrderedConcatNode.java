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

/**
 * Executes child scan nodes sequentially in a fixed order, exhausting one child
 * before moving to the next. Used by {@code $in + SORTBY} on the same indexed field:
 * children are EQ scans sorted by value, producing globally sorted results without
 * scanning the entire index.
 */
public class OrderedConcatNode extends AbstractLogicalNode implements LogicalNode {

    public OrderedConcatNode(int id, List<PipelineNode> children) {
        super(id, children);
    }

    List<PipelineNode> getOrderedChildren(QueryContext ctx) {
        return ctx.options().isReverse() ? children().reversed() : children();
    }

    @Override
    public void execute(QueryContext ctx) {
        PipelineNode activeChild = findActiveChild(ctx);

        if (activeChild == null) {
            ctx.getOrCreateExecutionState(id()).setExhausted(true);
            return;
        }

        DocumentLocationSink childSink = ctx.sinks().loadDocumentLocationSink(activeChild.id());
        if (childSink != null && childSink.size() > 0) {
            DocumentLocationSink ourSink = ctx.sinks().loadOrCreateDocumentLocationSink(id());
            for (DocumentLocation loc : childSink.entries()) {
                ourSink.append(loc);
            }
            childSink.clear();
        }

        ExecutionState activeState = ctx.getOrCreateExecutionState(activeChild.id());
        if (activeState.isExhausted()) {
            boolean allDone = true;
            for (PipelineNode child : children()) {
                if (!ctx.getOrCreateExecutionState(child.id()).isExhausted()) {
                    allDone = false;
                    break;
                }
            }
            if (allDone) {
                ctx.getOrCreateExecutionState(id()).setExhausted(true);
            }
        }
    }

    private PipelineNode findActiveChild(QueryContext ctx) {
        for (PipelineNode child : getOrderedChildren(ctx)) {
            if (!ctx.getOrCreateExecutionState(child.id()).isExhausted()) {
                return child;
            }
        }
        return null;
    }
}
