package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;

import java.util.List;

public interface PipelineNode {
    void execute(Transaction tr, PipelineContext ctx);
    List<PipelineNode> children();
}
