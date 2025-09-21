/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.pipeline.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class QueryExecutor {
    private final ReadExecutor readExecutor;
    private final DeleteExecutor deleteExecutor;
    private final UpdateExecutor updateExecutor;

    public QueryExecutor(BucketService service) {
        DocumentRetriever documentRetriever = new DocumentRetriever(service);
        CursorManager cursorManager = new CursorManager();
        PipelineEnv env = new PipelineEnv(service, documentRetriever, cursorManager);
        PipelineExecutor executor = new PipelineExecutor(env);
        this.readExecutor = new ReadExecutor(executor);
        this.deleteExecutor = new DeleteExecutor(executor);
        this.updateExecutor = new UpdateExecutor(executor);
    }

    public Map<Versionstamp, ByteBuffer> read(Transaction tr, QueryContext ctx) {
        return readExecutor.execute(tr, ctx);
    }

    public List<Versionstamp> delete(Transaction tr, QueryContext ctx) {
        return deleteExecutor.execute(tr, ctx);
    }

    public List<Versionstamp> update(Transaction tr, QueryContext ctx) {
        return updateExecutor.execute(tr, ctx);
    }
}
