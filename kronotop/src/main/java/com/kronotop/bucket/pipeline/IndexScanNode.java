package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.index.IndexDefinition;

import java.util.List;

public final class IndexScanNode extends AbstractScanNode {
    public IndexScanNode(int id, IndexDefinition index, List<Predicate> predicates) {
        super(id, index, predicates);
    }

    @Override
    public void execute(PipelineContext ctx, Transaction tr) {
        System.out.printf("IndexScanNode ==> %d, %s, %s%n", id(), index(), predicates());
        Predicate predicate = predicates().getFirst();
        DirectorySubspace subspace = ctx.getMetadata().indexes().getSubspace(index().selector());
        Cursor cursor = ctx.getCursor(id());
        IndexScanContext indexScanContext = new IndexScanContext(id(), subspace, cursor, ctx.isReverse(), predicate, index());
        System.out.println(indexScanContext);
    }
}