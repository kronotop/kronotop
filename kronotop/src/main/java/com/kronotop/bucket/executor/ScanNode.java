package com.kronotop.bucket.executor;

import java.util.List;

public interface ScanNode extends PipelineNode {
    List<PredicateNode> predicates();
}