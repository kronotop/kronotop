package com.kronotop.bucket.executor;

import java.util.List;

// Primary & Secondary index scan and testing predicates.
public interface ScanNode extends PipelineNode {
    List<Predicate> predicates();
}