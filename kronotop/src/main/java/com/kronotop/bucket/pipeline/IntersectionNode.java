package com.kronotop.bucket.pipeline;

import java.util.List;

public class IntersectionNode extends AbstractLogicalNode implements LogicalNode{
    public IntersectionNode(int id, List<PipelineNode> children) {
        super(id, children);
    }

    @Override
    public void execute(PipelineContext ctx) {
        System.out.println("EXECUTING AND");
    }
}
