package com.kronotop.bucket.pipeline;

import java.util.List;

public class DataFetchNodeImpl extends AbstractPipelineNode implements DataFetchNode {
    public DataFetchNodeImpl(int id) {
        super(id);
    }

    @Override
    public List<PipelineNode> children() {
        return List.of();
    }

    @Override
    public void execute(PipelineContext ctx) {

    }
}
