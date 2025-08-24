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

package com.kronotop.bucket.executor;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.index.IndexUtil;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;

public class PipelineContext {
    private final Context context;
    private final BucketMetadata metadata;
    private final PlanExecutorConfig config;
    private final CursorManager cursorManager;
    private final FilterEvaluator filterEvaluator;
    private final DocumentRetriever documentRetriever;
    private final IndexUtils indexUtils;
    private final SelectorCalculator selectorCalculator;

    private final ConcurrentHashMap<Integer, LinkedHashMap<Versionstamp, ByteBuffer>> output = new ConcurrentHashMap<>();

    public PipelineContext(Context context, BucketMetadata metadata, PlanExecutorConfig config) {
        this.context = context;
        this.metadata = metadata;
        this.config = config;

        BucketService bucketService = context.getService(BucketService.NAME);
        this.cursorManager = new CursorManager();
        this.filterEvaluator = new FilterEvaluator();
        this.documentRetriever = new DocumentRetriever(bucketService);
        this.indexUtils = new IndexUtils();
        this.selectorCalculator = new SelectorCalculator(indexUtils, cursorManager);
    }

    public Context context() {
        return context;
    }

    public BucketMetadata getMetadata() {
        return metadata;
    }

    public PlanExecutorConfig config() {
        return config;
    }

    public SelectorCalculator selectorCalculator() {
        return selectorCalculator;
    }

    public CursorManager cursorManager() {
        return cursorManager;
    }

    public FilterEvaluator filterEvaluator() {
        return filterEvaluator;
    }

    public DocumentRetriever documentRetriever() {
        return documentRetriever;
    }

    public IndexUtils indexUtils() {
        return indexUtils;
    }

    public LinkedHashMap<Versionstamp, ByteBuffer> getResults(int nodeId) {
        return output.computeIfAbsent(nodeId, (ignored) -> new LinkedHashMap<>());
    }
}

