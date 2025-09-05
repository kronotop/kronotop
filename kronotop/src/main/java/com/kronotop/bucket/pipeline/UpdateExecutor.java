package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;

import java.util.List;

public final class UpdateExecutor implements Executor<List<Versionstamp>> {
    @Override
    public List<Versionstamp> execute(Transaction tr, QueryContext ctx) {
        return null;
    }
}
