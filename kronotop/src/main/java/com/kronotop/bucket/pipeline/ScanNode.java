package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.index.IndexDefinition;

// Primary & Secondary index scan and testing predicates.
public interface ScanNode extends TransactionAwareNode {
    IndexDefinition index();
}